# QuickAds YouTube Creator Search Engine

A powerful, self-hosted web application to discover genuine YouTube creators for influencer marketing campaigns. Built for marketers who need to find real content creators, not official brand channels.

![Dashboard Preview](https://via.placeholder.com/800x400?text=QuickAds+Creator+Search)

## Why QuickAds?

Finding genuine YouTube creators for partnerships is hard. Most tools return official brand channels (Coursera, Udemy, Google) instead of individual influencers. QuickAds solves this with **smart creator detection** that filters out corporate channels and finds real people.

## Features

- **Creator-Focused Search**: Automatically filters out official/brand channels
- **Smart Detection**: Identifies individual creators vs corporate accounts
- **Multi-Region Support**: Search across 25+ countries (USA, India, Peru, UK, etc.)
- **Language Filtering**: Find creators in English, Hindi, Spanish, and more
- **Subscriber Filters**: Target the right audience size (micro to macro influencers)
- **Beautiful Dashboard**: Modern, responsive UI with statistics and charts
- **Export to CSV**: Download your creator database for outreach campaigns
- **Scheduled Scraping**: Automated hourly discovery of new creators
- **Self-Hosted**: Deploy on your own domain with Docker

## Quick Start

### Option 1: Local Development

```bash
# 1. Clone/navigate to the project
cd youtube-scraper

# 2. Create environment file
cp .env.example .env

# 3. Edit .env and add your YouTube API key
# Get one at: https://console.cloud.google.com/apis/credentials

# 4. Run the setup script
chmod +x run.sh
./run.sh
```

Open http://localhost:8000 in your browser.

### Option 2: Docker

```bash
# 1. Create .env file with your API key
cp .env.example .env
# Edit .env and add YOUTUBE_API_KEY

# 2. Build and run with Docker Compose
docker-compose up -d

# 3. View logs
docker-compose logs -f
```

## Getting a YouTube API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing)
3. Enable the **YouTube Data API v3**
4. Go to **Credentials** → **Create Credentials** → **API Key**
5. Copy the API key to your `.env` file

## Configuration

Edit `.env` file:

```env
# Required: Your YouTube API key
YOUTUBE_API_KEY=your_api_key_here

# Optional: Change the port (default: 8000)
PORT=8000

# Optional: Scraper interval in hours (default: 1)
SCHEDULER_INTERVAL_HOURS=1
```

## Deployment on Your Domain

### Using Docker (Recommended)

1. SSH into your server
2. Clone/upload the project
3. Create `.env` file with your API key
4. Run:
   ```bash
   docker-compose up -d
   ```
5. Set up a reverse proxy (nginx) to point your domain to port 8000

### Nginx Configuration Example

```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

### With SSL (Let's Encrypt)

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Get SSL certificate
sudo certbot --nginx -d yourdomain.com
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard UI |
| `/api/stats` | GET | Get statistics |
| `/api/channels` | GET | List channels (paginated) |
| `/api/channels/{id}` | DELETE | Delete a channel |
| `/api/queries` | GET | List search queries |
| `/api/queries` | POST | Add search query |
| `/api/queries/{id}` | PUT | Update search query |
| `/api/queries/{id}` | DELETE | Delete search query |
| `/api/scrape` | POST | Trigger manual scrape |
| `/api/history` | GET | Get scrape history |
| `/api/export` | GET | Export channels as CSV |

## Project Structure

```
youtube-scraper/
├── main.py              # FastAPI application
├── database.py          # SQLite database module
├── scraper.py           # YouTube API scraping logic
├── templates/
│   └── index.html       # Frontend UI
├── static/              # Static assets
├── requirements.txt     # Python dependencies
├── Dockerfile           # Docker configuration
├── docker-compose.yml   # Docker Compose config
├── .env.example         # Environment template
└── README.md           # This file
```

## Tech Stack

- **Backend**: FastAPI (Python)
- **Database**: SQLite
- **Frontend**: HTML + Tailwind CSS + Vanilla JS
- **Scheduler**: APScheduler
- **API**: YouTube Data API v3

## Troubleshooting

### "YouTube API key not configured"
Make sure your `.env` file contains a valid `YOUTUBE_API_KEY`.

### "Quota exceeded"
YouTube API has daily quotas. The scraper uses ~100 units per search query.
Default quota is 10,000 units/day. Reduce search queries or request higher quota.

### Database locked
Only one instance should run at a time. Check if another process is using the database.

## License

MIT License - feel free to use and modify for your needs.
