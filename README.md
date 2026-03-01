# PE/M&A Daily Digest

Resumo diário automatizado de notícias de Private Equity e M&A, entregue via Telegram às **05h30 BRT**, rodando gratuitamente no GitHub Actions.

## Como funciona

```
GitHub Actions cron (08:30 UTC = 05:30 BRT)
         |
    digest.py
         |
    +----+----+
    |         |
  Google    RSS feeds
  News RSS  especializados
    |         |
    +----+----+
         |
    Filtro de data (≤48h, ano≥2026)
    Filtro de relevância PE/M&A
    Agrupamento por região
         |
    Groq API (llama-3.3-70b-versatile)
    Prompt em PT-BR → resumo analítico
         |
    Telegram Bot API
```

## Setup

### 1. Fork / clone este repositório no GitHub

### 2. Crie os secrets em Settings → Secrets → Actions

| Secret | Como obter |
|--------|-----------|
| `GROQ_API_KEY` | [console.groq.com](https://console.groq.com) → API Keys → Create |
| `TELEGRAM_BOT_TOKEN` | Telegram → @BotFather → /newbot |
| `TELEGRAM_CHAT_ID` | Telegram → @userinfobot → /start |

### 3. Habilite GitHub Actions

Vá em **Actions** → habilite workflows para o repositório.

### 4. Teste manual

Actions → **Daily PE/M&A Digest** → **Run workflow**

## Fontes monitoradas

### Google News RSS (primárias)
- Private equity Brasil fusões aquisições
- M&A acquisitions brazil deal
- Private equity latin america latam deal
- Merger acquisition united states buyout
- Private equity europe deal acquisition

### RSS Especializados
- Portal Fusões & Aquisições
- Valor Econômico (Finanças + Empresas)
- InfoMoney Mercados
- Exame
- GlobeNewswire M&A
- PR Newswire M&A
- AltAssets PE (Global + Europa)
- Reuters Business

## Filtro de frescor

- **Dias úteis**: artigos das últimas 48h
- **Segunda-feira**: artigos das últimas 72h (cobre fim de semana)
- **Artigos de 2025 ou anteriores**: descartados explicitamente

## Custo total: $0

- GitHub Actions: gratuito (repo público = minutos ilimitados)
- Groq API: gratuito (free tier cobre 1 chamada/dia)
- Telegram Bot: gratuito

## Exemplo de saída

```
📊 PE/M&A Digest — 28/02/2026
Resumo executivo — 23 notícias monitoradas

🇧🇷 BRASIL
• Advent International adquire participação na XYZ
  Deal avaliado em R$ 800M (8x EV/EBITDA). Advent mantém estratégia de
  consolidação no setor de saúde no Brasil.

🌎 LATAM
• KKR fecha buyout de empresa colombiana por USD 450M
  ...

📌 Fontes: 23 artigos · Gerado às 05:31 BRT
```

## Depuração

Em caso de falha, o arquivo `digest_debug.log` é salvo como artefato no GitHub Actions por 3 dias (Actions → workflow run → Artifacts).
