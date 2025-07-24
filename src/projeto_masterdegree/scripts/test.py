import os
import time
import datetime
from youtube_transcript_api import YouTubeTranscriptApi, RequestBlocked
from youtube_transcript_api.proxies import GenericProxyConfig

USER   = "bvotxoyg-rotate"
PASS   = "b4blova7leta"
DOMAIN = "p.webshare.io"
PORT   = 80

proxy_cfg = GenericProxyConfig(
    http_url  = f"http://{USER}:{PASS}@{DOMAIN}:{PORT}",
    https_url = f"http://{USER}:{PASS}@{DOMAIN}:{PORT}"
)

ytt = YouTubeTranscriptApi(proxy_config=proxy_cfg)

video_id = "oSfll8eNHCk"
now       = datetime.datetime.now()
end_time  = now.replace(hour=15, minute=0, second=0, microsecond=0)

if now >= end_time:
    print("Já são 15h ou mais — não entra no loop.")
    exit(0)

while True:
    now = datetime.datetime.now()
    if now >= end_time:
        print("Chegou às 15h — encerrando monitoramento.")
        break

    try:
        segments = ytt.fetch(video_id, languages=["pt","en"])
        print("Bloqueio encerrado!")  
        break
    except RequestBlocked:
        print(f"[{now.strftime('%H:%M')}] Ainda bloqueado, tentando novamente em 10 minutos...")
        time.sleep(10 * 60)

print("Script finalizado.")