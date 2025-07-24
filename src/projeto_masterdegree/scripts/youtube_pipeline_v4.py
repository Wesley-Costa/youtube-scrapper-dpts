"""
youtube_pipeline_v4.py

YouTube Data Pipeline Refatorado - Versão 4
Integração de proxy_config da youtube-transcript-api para evitar bans.
"""

import os
import json
import argparse
import time
import random
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig
import urllib.parse
import requests
from bs4 import BeautifulSoup


def setup_paths(data_dir: str):
    base_dir = Path(data_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return {
        'state': base_dir / 'state.json',
        'base':  base_dir / 'base_partial.csv',
        'meta':  base_dir / 'meta_partial.csv',
        'trans': base_dir / 'trans_partial.csv',
        'final': base_dir / 'youtube_data_full.csv',
        'log':   base_dir / 'pipeline.log',
    }


def setup_logging(log_path: Path):
    logger = logging.getLogger('youtube_pipeline_v4')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=2)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)
    return logger


def load_state(state_path: Path):
    if state_path.exists():
        return json.loads(state_path.read_text(encoding='utf-8'))
    return {"last_channel_index": 0, "videos_metadata_done": [], "transcripts_done": []}


def save_state(state: dict, state_path: Path):
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')


def ensure_csv(path: Path, columns: list):
    if not path.exists():
        pd.DataFrame(columns=columns).to_csv(path, index=False)


def append_df(df: pd.DataFrame, path: Path):
    if df is None or df.empty:
        return
    header = not path.exists()
    df.to_csv(path, mode='a', index=False, header=header)


def resolve_channel_id(youtube, url: str, logger):
    parsed = urllib.parse.urlparse(url)
    segments = [seg for seg in parsed.path.split("/") if seg]
    # /channel/UC... direct
    if segments and segments[0].lower() == 'channel' and len(segments) >= 2:
        return segments[1]
    # /user/legacy or /c/custom
    if segments and segments[0].lower() in ('user','c') and len(segments) >= 2:
        identifier = segments[1]
        try:
            if segments[0].lower() == 'user':
                resp = youtube.channels().list(part="id", forUsername=identifier).execute()
                items = resp.get('items',[])
                if items:
                    return items[0]['id']
            # fallback search
            resp = youtube.search().list(part="snippet", q=identifier, type="channel", maxResults=1).execute()
            items = resp.get('items',[])
            if items:
                return items[0]['snippet']['channelId']
        except Exception as e:
            logger.warning(f"Erro resolvendo canal '{identifier}': {e}")
        return None
    # @handle
    if segments and segments[0].startswith('@'):
        handle = segments[0].lstrip('@')
        try:
            resp = youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1).execute()
            items = resp.get('items',[])
            if items:
                return items[0]['snippet']['channelId']
        except Exception as e:
            logger.error(f"Erro busca handle '{handle}': {e}")
        return None
    # fallback HTML scrape
    try:
        html = requests.get(url, timeout=10).text
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", itemprop="channelId")
        if tag:
            return tag['content']
    except Exception as e:
        logger.error(f"Erro parse HTML '{url}': {e}")
    logger.error(f"Não foi possível extrair channelId de {url}")
    return None


def get_uploads_playlist_id(youtube, channel_id: str):
    resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']


def get_all_videos(youtube, playlist_id: str, max_videos=None):
    videos, token = [], None
    while True:
        resp = youtube.playlistItems().list(
            part="snippet", playlistId=playlist_id, maxResults=50, pageToken=token
        ).execute()
        for it in resp.get('items',[]):
            videos.append({
                'video_id': it['snippet']['resourceId']['videoId'],
                'title':    it['snippet']['title']
            })
        token = resp.get('nextPageToken')
        if not token or (max_videos and len(videos)>=max_videos):
            break
    return videos[:max_videos] if max_videos else videos


def fetch_transcript_records(ytt_api, video_id: str, languages: list, delay: float, backoff_max: float, logger):
    backoff = delay
    while True:
        try:
            transcript = ytt_api.fetch(video_id, languages=languages)
            records = [{
                'video_id': video_id,
                'start': seg.start,
                'duration': seg.duration,
                'text': seg.text
            } for seg in transcript]
            return pd.DataFrame(records)
        except (TranscriptsDisabled, NoTranscriptFound):
            logger.warning(f"Transcrição indisponível para {video_id}")
            return pd.DataFrame(columns=['video_id','start','duration','text'])
        except Exception as e:
            logger.error(f"Erro fetch transcript {video_id}: {e}")
            if backoff > backoff_max:
                logger.error(f"Backoff máximo atingido para {video_id}, pulando")
                return pd.DataFrame(columns=['video_id','start','duration','text'])
            logger.info(f"Backoff {backoff}s antes de retentar {video_id}")
            time.sleep(backoff)
            backoff *= 2


def stage1(youtube, channels, state, paths, logger, max_videos=None):
    for idx, row in enumerate(tqdm(channels.itertuples(), desc="Stage1 - canais"), start=1):
        if idx <= state['last_channel_index']:
            continue
        cid = resolve_channel_id(youtube, row.youtube, logger)
        state['last_channel_index'] = idx
        save_state(state, paths['state'])
        if not cid:
            continue
        try:
            playlist = get_uploads_playlist_id(youtube, cid)
            vids = get_all_videos(youtube, playlist, max_videos)
        except Exception as e:
            logger.error(f"[{idx}] Erro listar vídeos canal {cid}: {e}")
            continue
        if vids:
            df = pd.DataFrame([{  # primeiros metadados
                'pol_id': row.id,
                'pol_nome': row.nome,
                'pol_partido': row.siglaPartido,
                'channel_id': cid,
                'video_id': v['video_id'],
                'video_title': v['title']
            } for v in vids])
            append_df(df, paths['base'])
            logger.info(f"[{idx}] {len(vids)} vídeos salvos")
        else:
            logger.info(f"[{idx}] nenhum vídeo para canal {cid}")


def stage2(youtube, paths, state, logger):
    dfb = pd.read_csv(paths['base'])
    vids = dfb['video_id'].tolist()
    for i in tqdm(range(0,len(vids),50), desc="Stage2 - meta"):
        batch = vids[i:i+50]
        todo = [v for v in batch if v not in state['videos_metadata_done']]
        if not todo:
            continue
        try:
            resp = youtube.videos().list(part='snippet,statistics', id=','.join(todo)).execute()
            meta = []
            for it in resp.get('items',[]):
                sn, st = it['snippet'], it['statistics']
                meta.append({
                    'video_id': it['id'],
                    'published_at': sn.get('publishedAt'),
                    'description': sn.get('description',''),
                    'tags': sn.get('tags',[]),
                    'view_count': int(st.get('viewCount',0)),
                    'like_count': int(st.get('likeCount',0)),
                    'comment_count': int(st.get('commentCount',0))
                })
            if meta:
                append_df(pd.DataFrame(meta), paths['meta'])
                logger.info(f"Stage2: {len(meta)} meta registros salvos")
        except Exception as e:
            logger.error(f"Stage2 erro: {e}")
        state['videos_metadata_done'] += todo
        save_state(state, paths['state'])


def stage3(ytt_api, paths, state, logger, languages, delay, batch_size, batch_delay, backoff_max):
    dft = pd.read_csv(paths['trans']) if paths['trans'].exists() else pd.DataFrame()
    vids = pd.read_csv(paths['base'])['video_id'].unique().tolist()
    for idx, vid in enumerate(tqdm(vids, desc="Stage3 - transcrições"), start=1):
        if vid in state['transcripts_done']:
            continue
        df_t = fetch_transcript_records(ytt_api, vid, languages, delay, backoff_max, logger)
        append_df(df_t, paths['trans'])
        state['transcripts_done'].append(vid)
        save_state(state, paths['state'])
        time.sleep(delay)
        if idx % batch_size == 0:
            logger.info(f"Batch {idx}/{len(vids)} concluído, dormindo {batch_delay}s")
            time.sleep(batch_delay)


def stage4(paths, logger):
    dfb = pd.read_csv(paths['base'])
    dfm = pd.read_csv(paths['meta']) if paths['meta'].exists() else pd.DataFrame()
    dft = pd.read_csv(paths['trans']) if paths['trans'].exists() else pd.DataFrame(columns=['video_id','start','duration','text'])
    df_full = dft.groupby('video_id')['text'].apply(lambda xs: ' '.join(xs)).reset_index().rename(columns={'text':'full_transcript'})
    df_all = dfb.merge(dfm, on='video_id', how='left').merge(df_full, on='video_id', how='left')
    df_all.to_csv(paths['final'], index=False)
    logger.info(f"Stage4: final com {len(df_all)} registros")


def main():
    parser = argparse.ArgumentParser(description="YouTube Pipeline v4 com proxy_config")
    parser.add_argument(
        '-i', '--input',
        default="src/projeto_masterdegree/dados/data_social_net_dpts.csv",
        required=True,
        help='CSV canais (formato: id,nome,siglaPartido,youtube)'
    )
    parser.add_argument(
        '--data-dir',
        default='/home/wesley/mestrado/projeto_masterdegree/src/projeto_masterdegree/dados/data_scrapper_v4',
        help='Diretório de saída dos arquivos'
    )
    parser.add_argument(
        '-k', '--api-key',
        default="AIzaSyABlSUKSJSeJ_VedYE0c7qPHOmxZZbp3yY",
        help='YouTube Data API key'
    )
    parser.add_argument(
        '--max-channels',
        type=int,
        default=100,
        help='Máximo de canais a processar'
    )
    parser.add_argument(
        '--max-videos',
        type=int,
        default=10,
        help='Máximo de vídeos por canal'
    )
    parser.add_argument(
        '--webshare-user',
        default="bvotxoyg",
        help='Usuário Webshare (para proxy rotativo)'
    )
    parser.add_argument(
        '--webshare-pass',
        default="b4blova7leta",
        help='Senha Webshare (para proxy rotativo)'
    )
    parser.add_argument(
        '--locations',
        nargs='*',
        default=["de", "us"],
        help='Filtrar IPs Webshare por país, ex: --locations de us'
    )
    parser.add_argument(
        '--proxy-http',
        default="http://bvotxoyg:b4blova7leta@proxy.webshare.io:8080",
        help='URL do proxy HTTP genérico'
    )
    parser.add_argument(
        '--proxy-https',
        default="https://bvotxoyg:b4blova7leta@proxy.webshare.io:8443",
        help='URL do proxy HTTPS genérico'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay (em segundos) entre chamadas de transcrição'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Número de vídeos por lote antes de adicionar batch-delay'
    )
    parser.add_argument(
        '--batch-delay',
        type=float,
        default=60.0,
        help='Delay (em segundos) entre lotes de vídeos'
    )
    parser.add_argument(
        '--backoff-max',
        type=float,
        default=300.0,
        help='Tempo máximo (em segundos) para backoff exponencial'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Limpa todos os arquivos parciais e o estado antes de rodar'
    )

    args = parser.parse_args()

    paths = setup_paths(args.data_dir)
    logger = setup_logging(paths['log'])
    if args.reset:
        for p in paths.values():
            if p.exists(): p.unlink()
        logger.info('Reset completo')

    state = load_state(paths['state'])
    ensure_csv(paths['base'], ['pol_id','pol_nome','pol_partido','channel_id','video_id','video_title'])
    ensure_csv(paths['meta'], ['video_id','published_at','description','tags','view_count','like_count','comment_count'])
    ensure_csv(paths['trans'], ['video_id','start','duration','text'])

    api_key = args.api_key or os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        print('Erro: forneça --api-key ou defina YOUTUBE_API_KEY')
        return
    youtube = build('youtube','v3', developerKey=api_key)

    proxy_cfg = None
    if args.webshare_user and args.webshare_pass:
        proxy_cfg = WebshareProxyConfig(
            proxy_username=args.webshare_user,
            proxy_password=args.webshare_pass,
            filter_ip_locations=args.locations
        )
    elif args.proxy_http or args.proxy_https:
        proxy_cfg = GenericProxyConfig(
            http_url=args.proxy_http,
            https_url=args.proxy_https
        )
    ytt_api = YouTubeTranscriptApi(proxy_config=proxy_cfg) if proxy_cfg else YouTubeTranscriptApi()

    channels = pd.read_csv(args.input).dropna(subset=['youtube']).reset_index(drop=True)
    if args.max_channels:
        channels = channels.head(args.max_channels)

    stage1(youtube, channels, state, paths, logger, args.max_videos)
    stage2(youtube, paths, state, logger)
    stage3(
        ytt_api, paths, state, logger,
        languages=['pt','pt-BR','en'],
        delay=args.delay,
        batch_size=args.batch_size,
        batch_delay=args.batch_delay,
        backoff_max=args.backoff_max
    )
    stage4(paths, logger)

    print(f"Pipeline concluído. Arquivo final: {paths['final']}")

if __name__ == '__main__':
    main()

'''
poetry run youtube-pipeline \                                          
  --input src/projeto_masterdegree/dados/data_social_net_dpts.csv \
  --max-channels 100 \
  --max-videos 10
'''