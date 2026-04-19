from PIL import Image

def remove_exif(image_path, output_path):
    """
    The heart of the Privacy Guard: Strips all metadata from an image.
    """
    with Image.open(image_path) as img:
        data = list(img.getdata())
        image_without_exif = Image.new(img.mode, img.size)
        image_without_exif.putdata(data)
        image_without_exif.save(output_path)
    return True
