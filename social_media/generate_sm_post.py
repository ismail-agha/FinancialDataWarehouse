from PIL import Image, ImageDraw, ImageFont
import os, sys
from datetime import datetime

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from api.get_data import get_top_n_equity_gainers_losers

# Paths to the background image and font file
background_image_path = os.path.join(parent_dir, f"files/fdw_post_bg.png")
font_path = os.path.join(parent_dir, f"files/calibri-regular.ttf")

# Load the background image
background_image = Image.open(background_image_path)
bg_width, bg_height = background_image.size

# Load the TrueType font
initial_font_size = 15
font = ImageFont.truetype(font_path, initial_font_size)

# Define colors
green = "green"
red = "red"
black = "black"


def calculate_content_size(draw, font, entries, padding):
    line_height = draw.textbbox((0, 0), "Sample", font=font)[3] - draw.textbbox((0, 0), "Sample", font=font)[1]
    header_height = line_height + padding
    content_height = (len(entries) + 1) * (line_height + padding)

    # Determine the maximum length of the company names
    max_company_length = max(draw.textbbox((0, 0), str(company), font=font)[2] - draw.textbbox((0, 0), str(company), font=font)[0] for company, _, _ in entries)
    ltp_width = max(draw.textbbox((0, 0), str(ltp), font=font)[2] - draw.textbbox((0, 0), str(ltp), font=font)[0] for _, ltp, _ in entries)
    percent_width = max(draw.textbbox((0, 0), f"{percent_change:.1f}%", font=font)[2] - draw.textbbox((0, 0), f"{percent_change:.1f}%", font=font)[0] for _, _, percent_change in entries)

    text_width = max_company_length + ltp_width + percent_width + draw.textbbox((0, 0), "↑", font=font)[2] - draw.textbbox((0, 0), "↑", font=font)[0] + 3 * padding
    return text_width + 2 * padding, header_height + content_height + 2 * padding

def draw_top_equities(image_path, font_path, output_path, title, entries, arrow, arrow_color, trade_date):
    output_path = os.path.join(parent_dir, f"social_media/{output_path}")
    trade_date = convert_date(trade_date)

    padding = 20  # Define padding

    # Load the background image
    background_image = Image.open(image_path)
    bg_width, bg_height = background_image.size

    # Load the TrueType font
    initial_font_size = 15
    font = ImageFont.truetype(font_path, initial_font_size)

    # Create a drawing context
    draw = ImageDraw.Draw(background_image)

    # Increase font size until the content fills the background image
    while True:
        font = ImageFont.truetype(font_path, initial_font_size)
        content_width, content_height = calculate_content_size(draw, font, entries, 20)
        if content_width > bg_width * 0.7 or content_height > bg_height * 0.7:
            break
        initial_font_size += 1

    # Calculate the starting positions to center the content
    start_x = (bg_width - content_width) // 2
    start_y = (bg_height - content_height) // 2

    # # Draw the header
    # draw.text((start_x, start_y), f"Trade Date: {trade_date}", fill=black, font=font)
    # header_y = start_y + (draw.textbbox((0, 0), f"Trade Date: {trade_date}", font=font)[3] - draw.textbbox((0, 0), f"Trade Date: {trade_date}", font=font)[1]) + 30
    # draw.text((start_x, header_y), title, fill=black, font=font)

    # Draw the header
    draw.text((start_x, start_y), f"Trade Date: {trade_date}", fill=black, font=font)
    header_y = start_y + (draw.textbbox((0, 0), f"Trade Date: {trade_date}", font=font)[3] - draw.textbbox((0, 0), f"Trade Date: {trade_date}", font=font)[1]) + padding
    draw.text((start_x, header_y), f"{title} ({arrow})", fill=black, font=font)
    draw.text((start_x + draw.textbbox((0, 0), f"{title} (", font=font)[2], header_y), arrow, fill=arrow_color, font=font)

    # Draw each entry with the appropriate arrow
    y_position = header_y + (draw.textbbox((0, 0), title, font=font)[3] - draw.textbbox((0, 0), title, font=font)[1]) + 30  # Add extra padding below header

    max_company_length = max(draw.textbbox((0, 0), str(company), font=font)[2] - draw.textbbox((0, 0), str(company), font=font)[0] for company, _, _ in entries)
    ltp_width = max(draw.textbbox((0, 0), str(ltp), font=font)[2] - draw.textbbox((0, 0), str(ltp), font=font)[0] for _, ltp, _ in entries)
    percent_width = max(draw.textbbox((0, 0), f"{percent_change:.1f}%", font=font)[2] - draw.textbbox((0, 0), f"{percent_change:.1f}%", font=font)[0] for _, _, percent_change in entries)
    arrow_width = draw.textbbox((0, 0), arrow, font=font)[2] - draw.textbbox((0, 0), arrow, font=font)[0]

    line_spacing = 30  # Adjust line spacing here

    for i, (company, ltp, percent_change) in enumerate(entries):
        company_x = start_x
        arrow_x = company_x + max_company_length + padding
        ltp_x = arrow_x + arrow_width + padding
        percent_x = ltp_x + ltp_width + padding

        draw.text((company_x, y_position), f"{i+1}. {company}", fill=black, font=font)
        #draw.text((arrow_x, y_position), arrow, fill=arrow_color, font=font)
        draw.text((ltp_x, y_position), str(ltp), fill=black, font=font)
        draw.text((percent_x, y_position), f"{arrow} {percent_change:.1f}%", fill=arrow_color, font=font)

        y_position += (draw.textbbox((0, 0), str(company), font=font)[3] - draw.textbbox((0, 0), str(company), font=font)[1]) + line_spacing  # Add spacing between lines

    # Save the image
    background_image.save(output_path)
    #background_image.show()


def convert_date(date_str):
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Format the date as "30 JAN 2024"
    formatted_date = date_obj.strftime("%d %b %Y")

    # Get the day of the week
    day_of_week = date_obj.strftime("%A")

    # Return the formatted date with the day of the week
    return f"{formatted_date} ({day_of_week})"

if __name__ == "__main__":
    if len(sys.argv) > 1:
        trade_date = sys.argv[1]
    else:
        trade_date = datetime.now().strftime("%Y-%m-%d")

    print(f'trade_date = {trade_date}')

    exchange = 'BSE'

    # Get top gainers
    gainers_df = get_top_n_equity_gainers_losers(trade_date=trade_date, exchange=exchange, type='G', n=5)
    if len(gainers_df) != 0:
        gainers = [(row['issuer_name'], row['current_close'], row['percentage_change']) for _, row in gainers_df.iterrows()]
        draw_top_equities(background_image_path, font_path, 'smpost_top_gainers.png', f"Top 5 {exchange} Gainers", gainers, "↑", green,
                          trade_date)
    else:
        print(f'Gainers DF is empty for trade_date {trade_date}')

    # Get top losers
    losers_df = get_top_n_equity_gainers_losers(trade_date=trade_date, exchange=exchange, type='L', n=5)
    if len(losers_df) != 0:
        losers = [(row['issuer_name'], row['current_close'], row['percentage_change']) for _, row in losers_df.iterrows()]
        draw_top_equities(background_image_path, font_path, 'smpost_top_losers.png', f"Top 5 {exchange} Losers", losers, "↓", red, trade_date)
    else:
        print(f'Losers DF is empty for trade_date {trade_date}')

