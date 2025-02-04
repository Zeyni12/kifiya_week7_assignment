import os
import csv
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto

# Replace with your API credentials
api_id = '28024201'
api_hash = '6e9f67ae943f2f1984827d987ce5551e'
phone = '+251973120947'  # You need to log in once

# Create directory to save images if it doesn't exist
image_directory = "scraped_images"
os.makedirs(image_directory, exist_ok=True)

# Define the CSV file where the data will be saved
csv_file = 'telegram_messages.csv'

# Write the header for the CSV file
with open(csv_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Channel Title', 'Channel Username', 'Channel ID', 'Date', 'Message', 'Image Path'])  # Updated header

# Connect to Telegram
client = TelegramClient('session_name', api_id, api_hash)

async def main():
    await client.start()

    channel_usernames = [
        'DoctorsET', 'lobelia4cosmetics', 'yetenaweg', 'EAHCI'
    ]  # Add more channel usernames as needed

    for channel in channel_usernames:
        try:
            entity = await client.get_entity(channel)
            channel_id = entity.id  # Fetch channel ID
            channel_title = entity.title  # Fetch channel title

            messages = await client.get_messages(entity, limit=100)  # Get last 100 messages
            
            # Open CSV file in append mode to write messages and images
            with open(csv_file, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                for message in messages:
                    # Extract message text
                    message_text = message.text

                    # Check if message contains media (image)
                    image_path = None
                    if message.media and isinstance(message.media, MessageMediaPhoto):
                        # Download the image
                        file_name = f"{channel}_{message.id}.jpg"  # Use message ID as filename
                        image_path = os.path.join(image_directory, file_name)
                        await message.download_media(file=image_path)  # Save image

                    # Write message, image data, channel title, username, and ID to CSV
                    writer.writerow([channel_title, channel, channel_id, message.date, message_text, image_path])

        except Exception as e:
            print(f"Error fetching {channel}: {e}")

with client:
    client.loop.run_until_complete(main())

print(f"Messages and images have been saved to {csv_file} and {image_directory}")
