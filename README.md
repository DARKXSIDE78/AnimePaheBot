# 🎬 Auto Ongoing Anime Bot

An advanced Telegram bot for automatically downloading and sharing anime episodes from AnimePahe. Features fast upload speeds, intelligent queue management, and automated episode processing.

## ✨ Features

- **🚀 Fast Upload Speeds**: Utilizes Pyrofork library for 2-3x faster file uploads compared to standard methods
- **📊 Smart Queue System**: Automatically manages episodes waiting for all qualities to become available
- **🔄 Auto Processing**: Continuously monitors for new anime episodes and processes them automatically
- **📱 Multiple Quality Support**: Downloads and uploads episodes in 360p, 720p, and 1080p
- **🎯 Intelligent Retry**: Automatically retries episodes when missing qualities become available
- **📢 Channel Integration**: Posts anime with download buttons to main channel
- **💾 MongoDB Integration**: Stores processed episodes and prevents duplicates
- **⚡ Optimized Downloads**: Uses aria2c and multi-threaded downloading for maximum speed
- **🛡️ Flood Protection**: Built-in flood wait handling and retry mechanisms

## 🤖 Bot Developers

Created and maintained by:
- **[@Blakite_Ravii](https://t.me/Blakite_Ravii)** - Lead Developer
- **[@KamiKaito](https://t.me/KamiKaito)** - Co-Developer

## 🚀 Deployment

### Prerequisites
- Python 3.8+
- Telegram Bot Token
- MongoDB Database (optional but recommended)
- Telegram Channel(s) for posting

### Environment Variables

Create a `.env` file with the following variables:

```env
# Bot Configuration
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
ADMIN_CHAT_ID=your_admin_id
BOT_USERNAME=your_bot_username

# Channel Configuration
CHANNEL_ID=main_channel_id
CHANNEL_USERNAME=@main_channel
DUMP_CHANNEL_ID=dump_channel_id
DUMP_CHANNEL_USERNAME=@dump_channel

# Database
MONGO_URI=mongodb+srv://...

# Deployment
PORT=8010
```

### Installation

1. **Clone the repository**
```bash
git clone <repository_url>
cd anime-bot
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Install system dependencies**
```bash
# For faster downloads (optional)
apt-get install aria2

# For video processing
apt-get install ffmpeg
```

4. **Run the bot**
```bash
python bot2.py
```

### Docker Deployment

Build and run with Docker:

```bash
docker build -t anime-bot .
docker run -d --env-file .env anime-bot
```

### Koyeb Deployment

The bot is optimized for Koyeb deployment:
1. Connect your GitHub repository
2. Set environment variables in Koyeb dashboard
3. Deploy with automatic builds

## 🎮 Bot Commands

### Admin Commands
- `/start` - Start the bot and see main menu
- `/auto` - Toggle auto download on/off
- `/setinterval [seconds]` - Set check interval
- `/quality` - Manage quality settings
- `/status` - Check bot and queue status
- `/clearqueue` - Clear pending queue
- `/process` - Manually process latest episode

### Search & Download
- Send anime name to search
- Select from search results
- Choose episode and quality
- Bot handles download and upload

## 🔧 Technical Details

### Fast Upload System
- **Pyrofork Integration**: Uses Pyrofork client for parallel chunk uploads
- **Automatic Fallback**: Falls back to Telethon if Pyrofork fails
- **Progress Tracking**: Real-time upload progress with speed metrics
- **Chunk Optimization**: 4MB chunks for optimal upload speed

### Queue Management
- **Persistent Queue**: Stores pending episodes in JSON file
- **Smart Processing**: Checks pending queue before new episodes
- **Quality Tracking**: Monitors which qualities are available/missing
- **Auto Cleanup**: Removes old entries after 7 days

### Episode Processing Flow
1. **Check New Episodes**: Monitors AnimePahe for latest releases
2. **Quality Verification**: Checks if all enabled qualities are available
3. **Queue Management**: Adds to queue if qualities missing, processes if ready
4. **Download & Upload**: Uses fastest available method for each operation
5. **Channel Posting**: Creates formatted post with download buttons
6. **Cleanup**: Removes temporary files and updates database

## 📊 Performance Optimizations

- **Concurrent Downloads**: Up to 16 parallel connections
- **Smart Caching**: Caches anime information to reduce API calls
- **Batch Processing**: Processes multiple episodes efficiently
- **Resource Management**: Automatic cleanup of temporary files
- **Error Recovery**: Automatic retry with exponential backoff

## 🛠️ Troubleshooting

### Common Issues

1. **Slow Uploads**
   - Ensure Pyrofork is installed: `pip install pyrofork`
   - Check network connectivity
   - Verify Telegram API limits

2. **Episodes Skipping**
   - Bot automatically queues episodes with missing qualities
   - Check queue status with `/status` command
   - Episodes process when all qualities available

3. **Download Failures**
   - Install aria2 for better download speeds
   - Check disk space availability
   - Verify AnimePahe accessibility

## 📝 License

This project is for educational purposes only. Please respect copyright laws and support official anime releases.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📞 Support

For support and questions, contact the developers:
- [@Blakite_Ravii](https://t.me/Blakite_Ravii)
- [@KamiKaito](https://t.me/KamiKaito)

---

**Note**: This bot is continuously updated with new features and improvements. Stay tuned for updates!