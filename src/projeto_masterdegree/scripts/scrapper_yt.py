# import os
# import re
# import json
# import logging
# import pandas as pd
# from itertools import islice
# from tqdm import tqdm
# from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from bs4 import BeautifulSoup
# import requests

# # ---------------- CONFIGURAÇÕES ----------------
# API_KEY     = "AIzaSyBhkWSjF3FHL63GgHUyYZJPrKyRLZIHgA"
# BASE_DIR    = os.path.dirname(__file__)
# DATA_DIR    = "/home/wesley/mestrado/projeto_masterdegree/src/projeto_masterdegree/dados/data_scrapper/test"
# os.makedirs(DATA_DIR, exist_ok=True)
# STATE_FILE  = os.path.join(DATA_DIR, "state.json")

# P = {
#     "base":  os.path.join(DATA_DIR, "df_base_partial.csv"),
#     "meta":  os.path.join(DATA_DIR, "df_meta_partial.csv"),
#     "trans": os.path.join(DATA_DIR, "df_transcripts_partial.csv"),
#     "final": os.path.join(DATA_DIR, "todos_videos_politicos_com_transcricao.csv"),
#     "log":   os.path.join(DATA_DIR, "pipeline.log"),
# }

# # logging
# logging.basicConfig(
#     filename=P["log"],
#     filemode="a",
#     level=logging.INFO,
#     format="%(asctime)s %(levelname)s: %(message)s",
# )

# # cliente YouTube
# youtube = build("youtube", "v3", developerKey=API_KEY)

# def reset_partials():
#     """Remove todos os parciais e o state.json."""
#     for f in [STATE_FILE] + list(P.values()):
#         try:    os.remove(f)
#         except: pass

# def load_state():
#     if os.path.exists(STATE_FILE):
#         return json.load(open(STATE_FILE))
#     return {"last_channel_index": 0, "videos_metadata_done": [], "transcripts_done": []}

# def save_state(state):
#     with open(STATE_FILE, "w") as f:
#         json.dump(state, f, indent=2)

# def append_df(df, path, cols):
#     if df is None or df.empty:
#         return
#     header = not os.path.exists(path)
#     df.to_csv(path, mode="a", index=False, header=header, columns=cols)

# def batched(it, n):
#     it = iter(it)
#     while True:
#         batch = list(islice(it, n))
#         if not batch: break
#         yield batch

# def get_channel_id_from_url(url):
#     html = requests.get(url).text
#     soup = BeautifulSoup(html, "html.parser")
#     m = soup.find("meta", itemprop="channelId")
#     if m: return m["content"]
#     m2 = re.search(r'"channelId":"(UC[\w\-]{21}[AQgw])"', html)
#     if m2: return m2.group(1)
#     logging.error(f"Não foi possível extrair channelId: {url}")
#     return None

# def get_uploads_playlist_id(channel_id):
#     resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
#     return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

# def get_all_videos(pl_id, max_videos=None):
#     videos, token = [], None
#     while True:
#         resp = youtube.playlistItems().list(
#             part="snippet", playlistId=pl_id, maxResults=50, pageToken=token
#         ).execute()
#         for it in resp.get("items", []):
#             videos.append({
#                 "video_id": it["snippet"]["resourceId"]["videoId"],
#                 "title":    it["snippet"]["title"],
#             })
#         token = resp.get("nextPageToken")
#         if not token or (max_videos and len(videos) >= max_videos):
#             break
#     return videos[:max_videos] if max_videos else videos

# def stage1(channels, state, max_vids):
#     for idx, row in enumerate(tqdm(channels.itertuples(), desc="Stage1 - canais"), start=1):
#         if idx <= state["last_channel_index"]: continue
#         cid = get_channel_id_from_url(row.youtube)
#         if not cid:
#             logging.warning(f"[{idx}] Canal sem ID: {row.youtube}")
#             state["last_channel_index"] = idx; save_state(state)
#             continue
#         try:
#             pl = get_uploads_playlist_id(cid)
#         except (KeyError, HttpError) as e:
#             logging.error(f"[{idx}] Erro uploads playlist {cid}: {e}")
#             state["last_channel_index"] = idx; save_state(state)
#             continue

#         vids = get_all_videos(pl, max_vids)
#         if vids:
#             df = pd.DataFrame([{
#                 "pol_id":       row.id,
#                 "pol_nome":     row.nome,
#                 "pol_partido":  row.siglaPartido,
#                 "channel_id":   cid,
#                 "video_id":     v["video_id"],
#                 "video_title":  v["title"],
#             } for v in vids])
#             append_df(df, P["base"], df.columns.tolist())
#             logging.info(f"[{idx}] {len(vids)} vídeos salvos em base parcial")
#         else:
#             logging.info(f"[{idx}] Nenhum vídeo encontrado para {cid}")

#         state["last_channel_index"] = idx
#         save_state(state)

# def stage2(state):
#     df_base = pd.read_csv(P["base"])
#     vids = df_base["video_id"].tolist()

#     for i, batch in enumerate(tqdm(batched(vids, 50), desc="Stage2 - meta"), start=1):
#         todo = [v for v in batch if v not in state["videos_metadata_done"]]
#         if not todo: continue

#         resp = youtube.videos().list(
#             part="snippet,statistics", id=",".join(todo)
#         ).execute()
#         meta = []
#         for it in resp.get("items", []):
#             sn, st = it["snippet"], it["statistics"]
#             meta.append({
#                 "video_id":     it["id"],
#                 "published_at": sn["publishedAt"],
#                 "description":  sn["description"],
#                 "tags":         sn.get("tags", []),
#                 "view_count":   int(st.get("viewCount", 0)),
#                 "like_count":   int(st.get("likeCount", 0)),
#                 "comment_count":int(st.get("commentCount", 0)),
#             })
#         if meta:
#             dfm = pd.DataFrame(meta)
#             append_df(dfm, P["meta"], dfm.columns.tolist())
#             logging.info(f"Stage2 batch[{i}]: {len(meta)} meta salvos")

#         state["videos_metadata_done"] += todo
#         save_state(state)

# def stage3(state):
#     try:
#         df_base = pd.read_csv(P["base"])
#     except (FileNotFoundError, pd.errors.EmptyDataError):
#         logging.warning("Stage3: base parcial vazia, pulando")
#         return

#     vids = df_base["video_id"].unique().tolist()
#     ytt = YouTubeTranscriptApi()
#     for vid in tqdm(vids, desc="Stage3 - transcrições"):
#         if vid in state["transcripts_done"]: continue
#         try:
#             segs = ytt.fetch(vid, languages=["pt","pt-BR","en"])
#             logging.info(f"Transcrição obtida {vid}: {len(segs)} segmentos")
#             records = [{"video_id":vid,"start":s.start,"duration":s.duration,"text":s.text} for s in segs]
#             dfs = pd.DataFrame(records)
#             append_df(dfs, P["trans"], dfs.columns.tolist())
#         except TranscriptsDisabled:
#             logging.warning(f"Transcrições desabilitadas {vid}")
#         except NoTranscriptFound:
#             logging.warning(f"Sem transcrição {vid}")
#         except Exception as e:
#             logging.error(f"Erro fetch transcrição {vid}: {e}")
#         state["transcripts_done"].append(vid)
#         save_state(state)

# def stage4():
#     try:
#         dfb = pd.read_csv(P["base"])
#     except:
#         logging.error("Stage4: não há base, abortando")
#         return
#     try:
#         dfm = pd.read_csv(P["meta"])
#     except:
#         dfm = pd.DataFrame()
#     try:
#         dft = pd.read_csv(P["trans"])
#     except:
#         dft = pd.DataFrame(columns=["video_id","start","duration","text"])

#     df_full = (
#         dft.groupby("video_id")["text"]
#            .apply(lambda xs: " ".join(xs))
#            .reset_index()
#            .rename(columns={"text":"full_transcript"})
#     )
#     df_all = dfb.merge(dfm, on="video_id", how="left")
#     df_all = df_all.merge(df_full, on="video_id", how="left")
#     df_all["published_at"] = pd.to_datetime(df_all["published_at"], errors="coerce")
#     df_all.to_csv(P["final"], index=False)
#     logging.info(f"Stage4: final com {len(df_all)} linhas")

# def main():
#     import argparse
#     p = argparse.ArgumentParser(description="YouTube pipeline com reset")
#     p.add_argument("-i","--input",    required=True, help="CSV canais com id,nome,siglaPartido,youtube")
#     p.add_argument("--max-channels",  type=int, default=None, help="Máx. canais")
#     p.add_argument("--max-videos",    type=int, default=None, help="Máx. vídeos por canal")
#     p.add_argument("--reset", action="store_true", help="Limpa parciais")
#     args = p.parse_args()

#     if args.reset:
#         logging.info("Reset acionado — removendo parciais")
#         reset_partials()

#     state = load_state()
#     df = pd.read_csv(args.input)
#     channels = df[["id","nome","siglaPartido","youtube"]].dropna(subset=["youtube"]).reset_index(drop=True)
#     if args.max_channels:
#         channels = channels.head(args.max_channels)

#     stage1(channels, state, args.max_videos)
#     stage2(state)
#     stage3(state)
#     stage4()
#     print("Pipeline concluído")

# if __name__ == "__main__":
#     main()


# ******

import os
import re
import json
import logging
import pandas as pd
from itertools import islice
from tqdm import tqdm
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    NoTranscriptFound,
    TranscriptsDisabled,
)
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
import requests

# ---------------- CONFIGURAÇÕES ----------------
API_KEY = "AIzaSyBhkWSjF3FHL63GgHUYyYZJPrKyRLZIHgA"
if not API_KEY:
    raise RuntimeError(
        "Defina a variável de ambiente YOUTUBE_API_KEY com sua chave de API do YouTube."
    )
youtube = build("youtube", "v3", developerKey=API_KEY)

STATE_FILE = "state.json"
DATA_DIR = "/home/wesley/mestrado/projeto_masterdegree/src/projeto_masterdegree/dados/data_scrapper"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

P = {
    "base": os.path.join(DATA_DIR, "df_base_partial.csv"),
    "meta": os.path.join(DATA_DIR, "df_meta_partial.csv"),
    "trans": os.path.join(DATA_DIR, "df_transcripts_partial.csv"),
    "final": os.path.join(DATA_DIR, "todos_videos_politicos_com_transcricao.csv"),
    "log": os.path.join(DATA_DIR, "pipeline.log"),
}

logging.basicConfig(
    filename=P["log"],
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE))
    return {"last_channel_index": 0, "videos_metadata_done": [], "transcripts_done": []}


def save_state(state):
    json.dump(state, open(STATE_FILE, "w"), indent=2)


def append_df(df, path, cols):
    header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=header, columns=cols)


def batched(it, n):
    it = iter(it)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


def get_channel_id_from_url(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")
    m = soup.find("meta", itemprop="channelId")
    if m:
        return m["content"]
    m2 = re.search(r'"channelId":"(UC[\w\-]{21}[AQgw])"', html)
    if m2:
        return m2.group(1)
    logging.error(f"Não foi possível extrair channelId: {url}")
    return None


def get_uploads_playlist_id(channel_id):
    resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_all_videos(pl_id, max_videos=None):
    videos, token = [], None
    while True:
        resp = (
            youtube.playlistItems()
            .list(part="snippet", playlistId=pl_id, maxResults=50, pageToken=token)
            .execute()
        )
        for it in resp.get("items", []):
            videos.append(
                {
                    "video_id": it["snippet"]["resourceId"]["videoId"],
                    "title": it["snippet"]["title"],
                }
            )
        token = resp.get("nextPageToken")
        if not token or (max_videos and len(videos) >= max_videos):
            break
    return videos[:max_videos] if max_videos else videos


def stage1(channels, state, max_vids):
    for idx, row in enumerate(
        tqdm(channels.itertuples(), desc="Stage1 - canais"), start=1
    ):
        if idx <= state["last_channel_index"]:
            continue
        cid = get_channel_id_from_url(row.youtube)
        if not cid:
            logging.warning(f"Canal {row.youtube} sem channel_id, pulando.")
            state["last_channel_index"] = idx
            save_state(state)
            continue
        try:
            pl = get_uploads_playlist_id(cid)
        except (KeyError, HttpError) as e:
            logging.error(f"Erro ao obter uploads playlist para {cid}: {e}")
            state["last_channel_index"] = idx
            save_state(state)
            continue
        vids = get_all_videos(pl, max_vids)
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
        append_df(df, P["base"], df.columns.tolist())
        state["last_channel_index"] = idx
        save_state(state)
        logging.info(f"Canais[{idx}]: {len(vids)} videos")


def stage2(state):
    vids = pd.read_csv(P["base"])["video_id"].tolist()
    for i, batch in enumerate(tqdm(batched(vids, 50), desc="Stage2 - meta"), start=1):
        todo = [v for v in batch if v not in state["videos_metadata_done"]]
        if not todo:
            continue
        resp = (
            youtube.videos()
            .list(part="snippet,statistics", id=",".join(todo))
            .execute()
        )
        meta = []
        for it in resp.get("items", []):
            sn, st = it["snippet"], it["statistics"]
            meta.append(
                {
                    "video_id": it["id"],
                    "published_at": sn["publishedAt"],
                    "description": sn["description"],
                    "tags": sn.get("tags", []),
                    "view_count": int(st.get("viewCount", 0)),
                    "like_count": int(st.get("likeCount", 0)),
                    "comment_count": int(st.get("commentCount", 0)),
                }
            )
        if meta:
            append_df(pd.DataFrame(meta), P["meta"], list(meta[0].keys()))
        state["videos_metadata_done"] += todo
        save_state(state)
        logging.info(f"Meta batch[{i}]: {len(todo)} vídeos")


def stage3(state):
    vids = pd.read_csv(P["base"])["video_id"].unique().tolist()
    for vid in tqdm(vids, desc="Stage3 - transcrições"):
        if vid in state["transcripts_done"]:
            continue
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(vid)
            transcript = transcript_list.find_transcript(["pt", "pt-BR", "en"])
            segs = transcript.fetch()
        except (NoTranscriptFound, TranscriptsDisabled):
            segs = []
        except Exception as e:
            logging.error(f"Erro ao obter transcrição para {vid}: {e}")
            segs = []
        if segs:
            df = pd.DataFrame(segs)
            df.insert(0, "video_id", vid)
            append_df(df, P["trans"], df.columns.tolist())
        state["transcripts_done"].append(vid)
        save_state(state)
        logging.info(f"Transcrição[{vid}]: {len(segs)} segmentos")


def stage4():
    dfb = pd.read_csv(P["base"])
    dfm = pd.read_csv(P["meta"])
    try:
        df_t = pd.read_csv(P["trans"])
    except pd.errors.EmptyDataError:
        df_t = pd.DataFrame(columns=["video_id", "start", "duration", "text"])
    df_full = (
        df_t.groupby("video_id")["text"]
        .apply(lambda xs: " ".join(xs))
        .reset_index()
        .rename(columns={"text": "full_transcript"})
    )
    df_all = dfb.merge(dfm, on="video_id", how="left").merge(
        df_full, on="video_id", how="left"
    )
    df_all["published_at"] = pd.to_datetime(df_all["published_at"])
    df_all.to_csv(P["final"], index=False)
    logging.info(f"Final: {df_all.shape[0]} linhas")


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="YouTube pipeline com suporte a teste e reset"
    )
    p.add_argument(
        "-i",
        "--input",
        required=True,
        help="CSV com colunas id,nome,siglaPartido,youtube",
    )
    p.add_argument(
        "--max-channels",
        type=int,
        default=None,
        help="Número máximo de canais a processar",
    )
    p.add_argument(
        "--max-videos", type=int, default=None, help="Número máximo de vídeos por canal"
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Limpa estado e arquivos parciais antes de iniciar",
    )
    args = p.parse_args()

    state = load_state()
    df = pd.read_csv(args.input)
    channels = (
        df[["id", "nome", "siglaPartido", "youtube"]]
        .dropna(subset=["youtube"])
        .reset_index(drop=True)
    )
    if args.max_channels:
        channels = channels.head(args.max_channels)
    stage1(channels, state, args.max_videos)
    stage2(state)
    stage3(state)
    stage4()
    print("Pipeline concluído")


if __name__ == "__main__":
    main()
