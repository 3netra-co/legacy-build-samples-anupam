from moviepy import VideoFileClip
from transformers import BlipProcessor, BlipForConditionalGeneration
from PIL import Image
import torch

# Load BLIP captioning model
processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")

def extract_caption(video_path):
    # Get frame
    clip = VideoFileClip(video_path)
    frame = clip.get_frame(clip.duration / 2)
    image = Image.fromarray(frame)

    # Caption it
    inputs = processor(images=image, return_tensors="pt")
    out = model.generate(**inputs)
    caption = processor.decode(out[0], skip_special_tokens=True)
    return caption

print(extract_caption("Your known MP4 path"))