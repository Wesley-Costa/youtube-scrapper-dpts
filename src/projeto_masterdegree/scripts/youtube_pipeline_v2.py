import os
import re
import json
import argparse
import urllib.parse
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from itertools import islice
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    NoTranscriptFound,
    TranscriptsDisabled,
)
import requests
from bs4 import BeautifulSoup


def setup_paths(data_dir: str):
    base_dir = Path(data_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return {
        "state": base_dir / "state.json",
        "base": base_dir / "base_partial.csv",
        "meta": base_dir / "meta_partial.csv",
        "trans": base_dir / "trans_partial.csv",
        "final": base_dir / "youtube_data_full.csv",
        "log": base_dir / "pipeline.log",
    }


def setup_logging(log_path: Path):
    logger = logging.getLogger("youtube_pipeline")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=2)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    return logger


def load_state(state_path: Path):
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {"last_channel_index": 0, "videos_metadata_done": [], "transcripts_done": []}


def save_state(state: dict, state_path: Path):
    state_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def ensure_csv(path: Path, columns: list):
    if not path.exists():
        pd.DataFrame(columns=columns).to_csv(path, index=False)


def append_df(df: pd.DataFrame, path: Path):
    if df is None or df.empty:
        return
    header = not path.exists()
    df.to_csv(path, mode="a", index=False, header=header)


def resolve_channel_id(youtube, url: str, logger):
    parsed = urllib.parse.urlparse(url)
    segments = [seg for seg in parsed.path.split("/") if seg]
    # Direct channel ID
    if segments and segments[0].lower() == "channel" and len(segments) >= 2:
        return segments[1]
    # Legacy user or custom URL
    if segments and segments[0].lower() in ("user", "c") and len(segments) >= 2:
        identifier = segments[1]
        # Try legacy username API
        if segments[0].lower() == "user":
            try:
                resp = (
                    youtube.channels().list(part="id", forUsername=identifier).execute()
                )
                items = resp.get("items", [])
                if items:
                    return items[0]["id"]
            except Exception as e:
                logger.warning(f"Não conseguiu resolver forUsername {identifier}: {e}")
        # Fallback search
        try:
            resp = (
                youtube.search()
                .list(part="snippet", q=identifier, type="channel", maxResults=1)
                .execute()
            )
            items = resp.get("items", [])
            if items:
                return items[0]["snippet"]["channelId"]
        except Exception as e:
            logger.error(f"Erro na busca por canal '{identifier}': {e}")
        return None
    # Handles with @
    if segments and segments[0].startswith("@"):
        handle = segments[0].lstrip("@")
        try:
            resp = (
                youtube.search()
                .list(part="snippet", q=handle, type="channel", maxResults=1)
                .execute()
            )
            items = resp.get("items", [])
            if items:
                return items[0]["snippet"]["channelId"]
        except Exception as e:
            logger.error(f"Erro na busca por handle '{handle}': {e}")
        return None
    # Fallback HTML parse
    try:
        html = requests.get(url, timeout=10).text
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", itemprop="channelId")
        if tag:
            return tag["content"]
    except Exception as e:
        logger.error(f"Erro ao parsear HTML para '{url}': {e}")
    logger.error(f"Não foi possível extrair channelId de {url}")
    return None


def get_uploads_playlist_id(youtube, channel_id: str):
    resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_all_videos(youtube, playlist_id: str, max_videos=None):
    videos, token = [], None
    while True:
        resp = (
            youtube.playlistItems()
            .list(
                part="snippet", playlistId=playlist_id, maxResults=50, pageToken=token
            )
            .execute()
        )
        for item in resp.get("items", []):
            videos.append(
                {
                    "video_id": item["snippet"]["resourceId"]["videoId"],
                    "title": item["snippet"]["title"],
                }
            )
        token = resp.get("nextPageToken")
        if not token or (max_videos and len(videos) >= max_videos):
            break
    return videos[:max_videos] if max_videos else videos


def fetch_transcript_records(video_id: str, languages: list, logger):
    try:
        ytt = YouTubeTranscriptApi()
        transcript = ytt.fetch(video_id, languages=languages)
        records = [
            {
                "video_id": video_id,
                "start": seg.start,
                "duration": seg.duration,
                "text": seg.text,
            }
            for seg in transcript
        ]
        return pd.DataFrame(records)
    except TranscriptsDisabled:
        logger.warning(f"Transcrições desabilitadas para {video_id}")
    except NoTranscriptFound:
        logger.warning(f"Sem transcrição encontrada para {video_id}")
    except Exception as e:
        logger.error(f"Erro ao obter transcrição de {video_id}: {e}")
    return pd.DataFrame(columns=["video_id", "start", "duration", "text"])


def stage1(
    youtube, channels: pd.DataFrame, state: dict, paths: dict, logger, max_videos=None
):
    for idx, row in enumerate(
        tqdm(channels.itertuples(), desc="Stage1 - canais"), start=1
    ):
        if idx <= state["last_channel_index"]:
            continue
        cid = resolve_channel_id(youtube, row.youtube, logger)
        if not cid:
            state["last_channel_index"] = idx
            save_state(state, paths["state"])
            continue
        try:
            playlist = get_uploads_playlist_id(youtube, cid)
            vids = get_all_videos(youtube, playlist, max_videos)
        except Exception as e:
            logger.error(f"[{idx}] Erro ao listar vídeos do canal {cid}: {e}")
            state["last_channel_index"] = idx
            save_state(state, paths["state"])
            continue
        if vids:
            df = pd.DataFrame(
                [
                    {
                        "pol_id": row.id,
                        "pol_nome": row.nome,
                        "pol_partido": row.siglaPartido,
                        "channel_id": cid,
                        "video_id": v["video_id"],
                        "video_title": v["title"],
                    }
                    for v in vids
                ]
            )
            append_df(df, paths["base"])
            logger.info(f"[{idx}] {len(vids)} vídeos salvos em base parcial")
        else:
            logger.info(f"[{idx}] Nenhum vídeo encontrado para canal {cid}")
        state["last_channel_index"] = idx
        save_state(state, paths["state"])


def stage2(youtube, paths: dict, state: dict, logger):
    base_df = pd.read_csv(paths["base"])
    vids = base_df["video_id"].tolist()
    for i in tqdm(range(0, len(vids), 50), desc="Stage2 - meta"):
        batch = vids[i : i + 50]
        todo = [v for v in batch if v not in state["videos_metadata_done"]]
        if not todo:
            continue
        try:
            resp = (
                youtube.videos()
                .list(part="snippet,statistics", id=",".join(todo))
                .execute()
            )
            meta = []
            for item in resp.get("items", []):
                sn, st = item["snippet"], item["statistics"]
                meta.append(
                    {
                        "video_id": item["id"],
                        "published_at": sn.get("publishedAt"),
                        "description": sn.get("description", ""),
                        "tags": sn.get("tags", []),
                        "view_count": int(st.get("viewCount", 0)),
                        "like_count": int(st.get("likeCount", 0)),
                        "comment_count": int(st.get("commentCount", 0)),
                    }
                )
            if meta:
                append_df(pd.DataFrame(meta), paths["meta"])
                logger.info(f"Stage2: {len(meta)} registros de meta salvos")
        except Exception as e:
            logger.error(f"Stage2 erro: {e}")
        state["videos_metadata_done"].extend(todo)
        save_state(state, paths["state"])


def stage3(paths: dict, state: dict, logger, languages=["pt", "pt-BR", "en"]):
    base_df = pd.read_csv(paths["base"])
    vids = base_df["video_id"].unique().tolist()
    for vid in tqdm(vids, desc="Stage3 - transcrições"):
        if vid in state["transcripts_done"]:
            continue
        df_trans = fetch_transcript_records(vid, languages, logger)
        append_df(df_trans, paths["trans"])
        state["transcripts_done"].append(vid)
        save_state(state, paths["state"])


def stage4(paths: dict, logger):
    df_base = pd.read_csv(paths["base"])
    df_meta = pd.read_csv(paths["meta"]) if paths["meta"].exists() else pd.DataFrame()
    df_trans = (
        pd.read_csv(paths["trans"])
        if paths["trans"].exists()
        else pd.DataFrame(columns=["video_id", "start", "duration", "text"])
    )
    df_full = (
        df_trans.groupby("video_id")["text"]
        .apply(lambda xs: " ".join(xs))
        .reset_index()
        .rename(columns={"text": "full_transcript"})
    )
    df_all = df_base.merge(df_meta, on="video_id", how="left").merge(
        df_full, on="video_id", how="left"
    )
    df_all.to_csv(paths["final"], index=False)
    logger.info(f"Stage4: arquivo final gerado com {len(df_all)} linhas")


def main():
    parser = argparse.ArgumentParser(description="YouTube pipeline refatorado v2")
    parser.add_argument("-i", "--input", required=True, help="CSV de canais")
    parser.add_argument(
        "--data-dir", default="./data_scrapper_v3", help="Diretório base para saída"
    )
    parser.add_argument(
        "--api-key",
        "-k",
        default="AIzaSyABlSUKSJSeJ_VedYE0c7qPHOmxZZbp3yY",
        help="Chave da YouTube Data API v3",
    )
    parser.add_argument(
        "--max-channels", type=int, default=None, help="Máximo de canais"
    )
    parser.add_argument(
        "--max-videos", type=int, default=None, help="Máximo de vídeos por canal"
    )
    parser.add_argument("--reset", action="store_true", help="Limpar parciais e estado")
    args = parser.parse_args()

    paths = setup_paths(args.data_dir)
    logger = setup_logging(paths["log"])
    if args.reset:
        for p in paths.values():
            if p.exists():
                p.unlink()
        logger.info("Reset acionado — arquivos parciais removidos")

    state = load_state(paths["state"])
    ensure_csv(
        paths["base"],
        ["pol_id", "pol_nome", "pol_partido", "channel_id", "video_id", "video_title"],
    )
    ensure_csv(
        paths["meta"],
        [
            "video_id",
            "published_at",
            "description",
            "tags",
            "view_count",
            "like_count",
            "comment_count",
        ],
    )
    ensure_csv(paths["trans"], ["video_id", "start", "duration", "text"])

    api_key = args.api_key or os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error(
            "Você deve fornecer --api-key ou definir a variável YOUTUBE_API_KEY"
        )
        print(
            "Erro: faltando chave da YouTube API. Use --api-key ou defina YOUTUBE_API_KEY."
        )
        return

    try:
        youtube = build("youtube", "v3", developerKey=api_key)
    except Exception as e:
        logger.error(f"Falha ao criar cliente YouTube API: {e}")
        print(f"Erro ao criar cliente YouTube API: {e}")
        return

    channels = pd.read_csv(args.input).dropna(subset=["youtube"]).reset_index(drop=True)
    if args.max_channels:
        channels = channels.head(args.max_channels)

    stage1(youtube, channels, state, paths, logger, args.max_videos)
    stage2(youtube, paths, state, logger)
    stage3(paths, state, logger)
    stage4(paths, logger)

    print(f"Pipeline concluído. Arquivo final: {paths['final']}")


if __name__ == "__main__":
    main()

# Keys: AIzaSyABlSUKSJSeJ_VedYE0c7qPHOmxZZbp3yY
#     : AIzaSyBhkWSjF3FHL63GgHUYyYZJPrKyRLZIHgA
