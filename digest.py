"""
PE/M&A Daily Digest
Busca, filtra e sumariza notícias de Private Equity e M&A,
entregando via Telegram às 05h30 BRT via GitHub Actions.
"""

import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import feedparser
import requests
from dateutil import parser as dateutil_parser
from groq import Groq

# ---------------------------------------------------------------------------
# Logging — stdout (Actions) + arquivo (artefato em caso de falha)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("digest_debug.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Feeds com parâmetro (region, url, is_dedicated)
# is_dedicated=True → bypass de filtro de keyword (todo artigo já é PE/M&A)
RSS_FEEDS = [
    # --- Grupo A: Google News RSS (fonte primária, sempre atual) ---
    ("Brasil",  "https://news.google.com/rss/search?q=private+equity+brasil+fus%C3%B5es+aquisi%C3%A7%C3%B5es&hl=pt-BR&gl=BR&ceid=BR:pt", False),
    ("Brasil",  "https://news.google.com/rss/search?q=M%26A+acquisitions+brazil+deal&hl=pt-BR&gl=BR&ceid=BR:pt", False),
    ("Latam",   "https://news.google.com/rss/search?q=private+equity+latin+america+latam+deal&hl=en-US&gl=US&ceid=US:en", False),
    ("EUA",     "https://news.google.com/rss/search?q=merger+acquisition+united+states+buyout&hl=en-US&gl=US&ceid=US:en", False),
    ("Europa",  "https://news.google.com/rss/search?q=private+equity+europe+deal+acquisition&hl=en-GB&gl=GB&ceid=GB:en", False),

    # --- Grupo B: RSS Especializados ---
    ("Brasil",  "https://fusoesaquisicoes.com/feed/", True),
    ("Brasil",  "https://valor.globo.com/rss/financas/", False),
    ("Brasil",  "https://valor.globo.com/rss/empresas/", False),
    ("Brasil",  "https://www.infomoney.com.br/mercados/feed/", False),
    ("Brasil",  "https://exame.com/feed/", False),
    ("Global",  "https://www.globenewswire.com/RssFeed/subjectcode/27-Mergers+and+Acquisitions", True),
    ("Global",  "https://www.prnewswire.com/rss/news-releases-list.rss?subjectCode=MA", True),
    ("Global",  "https://www.altassets.net/feed", True),
    ("Europa",  "https://www.altassets.net/category/news/international-pe-news/europe/feed", True),
    ("Global",  "https://feeds.reuters.com/reuters/businessNews", False),
]

TRANSACTION_KEYWORDS = [
    "private equity", "pe fund", "buyout", "leveraged buyout", "lbo",
    "merger", "acquisition", "aquisição", "aquisicao", "fusão", "fusao",
    "deal", "transaction", "takeover", "stake", "portfolio company",
    "m&a", "ma deal", "venture capital", "growth equity", "fund raise",
    "fundraising", "captação", "captacao", "investimento", "desinvestimento",
    "exit", "ipo", "secondary", "carve-out", "spin-off", "divestiture",
    "compra", "venda de participação", "venda de participacao",
]

ASSET_CLASS_KEYWORDS = [
    "fundo", "fundo de investimento", "gestora", "asset management",
    "gp", "lp", "limited partner", "general partner", "committed capital",
    "dry powder", "vintage", "irr", "moic", "ebitda",
]

GEO_KEYWORDS = {
    "Brasil":  ["brasil", "brazil", "brasileiro", "brasileira", "b3", "bovespa", "real", "r$",
                "são paulo", "rio de janeiro", "brasília", "rio", "sp", "rj"],
    "Latam":   ["latam", "latin america", "américa latina", "mexico", "colombia", "chile",
                "argentina", "peru", "bogotá", "santiago", "lima", "cdmx", "mexican", "colombian"],
    "EUA":     ["united states", "us ", "usa", "american", "new york", "wall street",
                "nasdaq", "nyse", "silicon valley", "delaware", "sec ", "dollar", "usd"],
    "Europa":  ["europe", "european", "london", "uk ", "united kingdom", "germany", "france",
                "paris", "frankfurt", "amsterdam", "milan", "spain", "nordic", "eu ", "euro"],
}

# Feeds cujos artigos são sempre PE/M&A — pula filtro de keyword
DEDICATED_FEEDS = {
    "fusoesaquisicoes.com",
    "globenewswire.com",
    "prnewswire.com",
    "altassets.net",
}

# ---------------------------------------------------------------------------
# Filtro de Data
# ---------------------------------------------------------------------------

def get_cutoff() -> datetime:
    """Janela de 48h (72h nas segundas para cobrir o fim de semana)."""
    now = datetime.now(timezone.utc)
    hours = 72 if now.weekday() == 0 else 48
    return now - timedelta(hours=hours)


def is_fresh(pub_time: datetime) -> bool:
    """Retorna True se o artigo está dentro da janela de frescor."""
    if pub_time is None:
        return False
    if pub_time.year < 2026:
        log.debug("Artigo descartado — ano anterior: %s", pub_time.year)
        return False
    return pub_time >= get_cutoff()


# ---------------------------------------------------------------------------
# Filtro de Relevância
# ---------------------------------------------------------------------------

def _contains_any(text: str, keywords: list[str]) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def _is_dedicated_feed(feed_url: str) -> bool:
    return any(domain in feed_url for domain in DEDICATED_FEEDS)


def is_relevant(title: str, summary: str, feed_url: str) -> bool:
    """
    Dois tiers:
    - Tier 1: feed dedicado → sempre relevante
    - Tier 2: verifica keywords de transação ou asset class no título/summary
    """
    if _is_dedicated_feed(feed_url):
        return True
    text = f"{title} {summary}"
    return _contains_any(text, TRANSACTION_KEYWORDS) or _contains_any(text, ASSET_CLASS_KEYWORDS)


# ---------------------------------------------------------------------------
# Detecção de Região
# ---------------------------------------------------------------------------

def detect_region(text: str, default: str) -> str:
    """Reclassifica artigos de feeds globais usando geo-keywords."""
    text_lower = text.lower()
    # Brasil tem prioridade — evita falsos positivos Latam
    for region in ["Brasil", "Latam", "EUA", "Europa"]:
        if any(kw in text_lower for kw in GEO_KEYWORDS[region]):
            return region
    return default


# ---------------------------------------------------------------------------
# Parse de Data de Publicação
# ---------------------------------------------------------------------------

def parse_pub_time(entry) -> datetime | None:
    """Converte feedparser struct_time ou string para datetime UTC-aware."""
    # Tenta published_parsed (struct_time UTC)
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            t = entry.published_parsed
            return datetime(*t[:6], tzinfo=timezone.utc)
        except Exception:
            pass

    # Tenta published como string com dateutil
    if hasattr(entry, "published") and entry.published:
        try:
            dt = dateutil_parser.parse(entry.published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    # Tenta updated_parsed
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            t = entry.updated_parsed
            return datetime(*t[:6], tzinfo=timezone.utc)
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Fetch de Artigos
# ---------------------------------------------------------------------------

def fetch_articles() -> dict[str, list[dict]]:
    """
    Itera RSS_FEEDS, filtra por frescor + relevância, deduplica por título.
    Retorna dict[region → list[article]].
    """
    articles_by_region: dict[str, list[dict]] = {
        "Brasil": [], "Latam": [], "EUA": [], "Europa": [], "Global": []
    }
    seen_titles: set[str] = set()
    total_fetched = 0
    total_kept = 0

    for region, url, is_dedicated in RSS_FEEDS:
        log.info("Buscando feed [%s] %s", region, url)
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "PE-Digest/1.0"})
        except Exception as exc:
            log.warning("Erro ao parsear feed %s: %s", url, exc)
            continue

        if feed.bozo and feed.bozo_exception:
            log.warning("Feed malformado %s: %s", url, feed.bozo_exception)

        entries = feed.entries or []
        log.info("  → %d entradas encontradas", len(entries))
        total_fetched += len(entries)

        for entry in entries:
            title = getattr(entry, "title", "") or ""
            summary = getattr(entry, "summary", "") or ""
            link = getattr(entry, "link", "") or ""

            # Deduplicação por título normalizado
            title_key = re.sub(r"\s+", " ", title.strip().lower())
            if title_key in seen_titles or len(title_key) < 10:
                continue

            # Filtro de data
            pub_time = parse_pub_time(entry)
            if not is_fresh(pub_time):
                log.debug("  Descartado (stale): %s | %s", pub_time, title[:60])
                continue

            # Filtro de relevância
            if not is_relevant(title, summary, url):
                log.debug("  Descartado (irrelevante): %s", title[:60])
                continue

            seen_titles.add(title_key)

            # Detecta região para feeds globais
            final_region = region
            if region == "Global":
                final_region = detect_region(f"{title} {summary}", "Global")

            article = {
                "title": title.strip(),
                "summary": summary.strip()[:400],
                "link": link,
                "pub_time": pub_time,
                "region": final_region,
                "source": url,
            }

            target = final_region if final_region in articles_by_region else "Global"
            articles_by_region[target].append(article)
            total_kept += 1
            log.info("  [+] [%s] %s", final_region, title[:80])

        # Pausa educada entre feeds
        time.sleep(0.5)

    log.info("Total buscados: %d | Total mantidos: %d", total_fetched, total_kept)
    return articles_by_region


# ---------------------------------------------------------------------------
# Geração do Resumo via Groq
# ---------------------------------------------------------------------------

def _format_articles_for_prompt(articles_by_region: dict[str, list[dict]]) -> str:
    """Formata os artigos em texto estruturado para o prompt."""
    lines = []
    brt = timezone(timedelta(hours=-3))
    now_brt = datetime.now(brt)
    lines.append(f"Data de referência: {now_brt.strftime('%d/%m/%Y')} (fuso BRT)")
    lines.append("")

    region_order = ["Brasil", "Latam", "EUA", "Europa", "Global"]
    total = 0

    for region in region_order:
        arts = articles_by_region.get(region, [])
        if not arts:
            continue
        lines.append(f"=== {region.upper()} ({len(arts)} artigos) ===")
        for a in arts:
            pub_str = a["pub_time"].strftime("%d/%m %H:%M UTC") if a["pub_time"] else "?"
            lines.append(f"Título: {a['title']}")
            lines.append(f"Data: {pub_str}")
            if a["summary"]:
                lines.append(f"Resumo: {a['summary']}")
            lines.append("")
            total += 1

    lines.insert(2, f"Total de artigos: {total}")
    return "\n".join(lines)


def generate_summary(articles_by_region: dict[str, list[dict]]) -> str:
    """Chama Groq llama-3.3-70b-versatile e retorna o digest formatado."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY não definida")

    total_articles = sum(len(v) for v in articles_by_region.values())
    if total_articles == 0:
        brt = timezone(timedelta(hours=-3))
        now_brt = datetime.now(brt)
        return (
            f"📊 *PE/M&A Digest — {now_brt.strftime('%d/%m/%Y')}*\n\n"
            "⚠️ Nenhuma notícia relevante encontrada nas últimas 48h\\.\n\n"
            f"📌 _Gerado às {now_brt.strftime('%H:%M')} BRT_"
        )

    articles_text = _format_articles_for_prompt(articles_by_region)
    brt = timezone(timedelta(hours=-3))
    now_brt = datetime.now(brt)
    date_str = now_brt.strftime("%d/%m/%Y")
    time_str = now_brt.strftime("%H:%M")

    system_prompt = (
        "Você é um analista sênior de Private Equity e M&A com 15 anos de experiência. "
        "Sua audiência são profissionais de investimento (GPs, LPs, banqueiros, advogados M&A). "
        "Responda SEMPRE em português do Brasil. "
        "Seja objetivo, analítico e direto — sem floreios jornalísticos. "
        "Use MarkdownV2 do Telegram: negrito com *texto*, itálico com _texto_, sem outros formatos. "
        "NUNCA invente informações — baseie-se apenas nos artigos fornecidos. "
        "Se um artigo não tiver detalhes suficientes, indique de forma concisa o que foi reportado."
    )

    user_prompt = f"""Analise os artigos de PE/M&A abaixo e produza o digest diário EXATAMENTE neste formato:

📊 *PE/M&A Digest — {date_str}*
_Resumo executivo — {total_articles} notícias monitoradas_

[Para cada região que tiver notícias, use a seção correspondente:]

🇧🇷 *BRASIL*
• *[Título ou empresa principal em negrito]*
  [Análise de 1-2 frases: valor do deal se disponível, compradores/vendedores, tese de investimento, impacto de mercado]

🌎 *LATAM*
• *[Título ou empresa principal]*
  [Análise]

🇺🇸 *EUA*
• *[Título ou empresa principal]*
  [Análise]

🇪🇺 *EUROPA*
• *[Título ou empresa principal]*
  [Análise]

📌 _Fontes: {total_articles} artigos · Gerado às {time_str} BRT_

REGRAS IMPORTANTES:
- Omita seções de regiões sem notícias
- Agrupe artigos sobre o mesmo deal em um único bullet
- Priorize notícias com valores financeiros concretos
- Mencione múltiplos e IRR apenas se explicitamente citados nos artigos
- Mantenha cada bullet conciso (máx. 3 linhas)
- Use linguagem técnica de mercado (PE, LBO, multiple, carve-out, etc.)

ARTIGOS:
{articles_text}"""

    client = Groq(api_key=api_key)
    log.info("Chamando Groq API com %d artigos...", total_articles)

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.4,
        max_tokens=2048,
    )

    summary = response.choices[0].message.content
    log.info("Groq respondeu com %d caracteres", len(summary))
    return summary


# ---------------------------------------------------------------------------
# Envio Telegram
# ---------------------------------------------------------------------------

def _escape_mdv2(text: str) -> str:
    """Escapa caracteres especiais do MarkdownV2 (exceto já formatados)."""
    # Caracteres que precisam de escape no MarkdownV2
    special = r"_*[]()~`>#+-=|{}.!"
    result = []
    i = 0
    while i < len(text):
        # Preserva formatação existente: *bold*, _italic_
        if text[i] in ("*", "_") and i + 1 < len(text):
            result.append(text[i])
            i += 1
        elif text[i] in special:
            result.append("\\" + text[i])
            i += 1
        else:
            result.append(text[i])
            i += 1
    return "".join(result)


def _split_message(text: str, chunk_size: int = 4000) -> list[str]:
    """Divide mensagem em chunks respeitando limite do Telegram."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    lines = text.split("\n")
    current = []
    current_len = 0

    for line in lines:
        if current_len + len(line) + 1 > chunk_size:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line) + 1

    if current:
        chunks.append("\n".join(current))

    return chunks


def send_telegram(text: str) -> bool:
    """
    Envia mensagem via Telegram Bot API.
    Tenta MarkdownV2 primeiro; em caso de erro de parse, reenvia como plain text.
    Retorna True se enviou com sucesso.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        raise ValueError("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID não definidos")

    base_url = f"https://api.telegram.org/bot{bot_token}"
    chunks = _split_message(text)
    success = True

    for i, chunk in enumerate(chunks, 1):
        log.info("Enviando chunk %d/%d (%d chars)...", i, len(chunks), len(chunk))

        # Tentativa 1: MarkdownV2
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }

        resp = requests.post(f"{base_url}/sendMessage", json=payload, timeout=30)

        if resp.ok:
            log.info("  Chunk %d enviado com sucesso (MarkdownV2)", i)
            time.sleep(0.3)
            continue

        log.warning("  MarkdownV2 falhou (%s): %s — tentando plain text", resp.status_code, resp.text[:200])

        # Fallback: plain text (remove markdown)
        plain = re.sub(r"[*_`\[\]()~>#+=|{}.!\\]", "", chunk)
        payload_plain = {
            "chat_id": chat_id,
            "text": plain,
            "disable_web_page_preview": True,
        }

        resp2 = requests.post(f"{base_url}/sendMessage", json=payload_plain, timeout=30)
        if resp2.ok:
            log.info("  Chunk %d enviado como plain text", i)
        else:
            log.error("  Falha ao enviar chunk %d: %s", i, resp2.text[:200])
            success = False

        time.sleep(0.3)

    return success


def send_error_notification(msg: str) -> None:
    """Envia notificação de erro best-effort (não lança exceção)."""
    try:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if not bot_token or not chat_id:
            return

        text = f"⚠️ *PE/M\&A Digest — ERRO*\n\n`{msg[:300]}`"
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "MarkdownV2"},
            timeout=15,
        )
    except Exception as exc:
        log.warning("Falha ao enviar notificação de erro: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    log.info("=== PE/M&A Digest iniciando ===")
    log.info("UTC agora: %s", datetime.now(timezone.utc).isoformat())
    log.info("Cutoff: %s", get_cutoff().isoformat())

    # 1. Fetch
    try:
        articles_by_region = fetch_articles()
        total = sum(len(v) for v in articles_by_region.items() if isinstance(v, tuple))
        total = sum(len(v) for v in articles_by_region.values())
        log.info("Artigos por região: %s", {k: len(v) for k, v in articles_by_region.items()})
    except Exception as exc:
        log.exception("Erro ao buscar artigos")
        send_error_notification(f"Erro fetch: {exc}")
        return 1

    # 2. Sumarização
    try:
        summary = generate_summary(articles_by_region)
        log.info("Resumo gerado (%d chars)", len(summary))
    except Exception as exc:
        log.exception("Erro ao gerar resumo")
        send_error_notification(f"Erro Groq: {exc}")
        return 1

    # 3. Envio Telegram
    try:
        ok = send_telegram(summary)
        if not ok:
            log.error("Falha parcial no envio Telegram")
            return 1
        log.info("=== Digest enviado com sucesso ===")
    except Exception as exc:
        log.exception("Erro ao enviar Telegram")
        send_error_notification(f"Erro Telegram: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
