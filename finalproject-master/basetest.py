import base64
def get_base64_encoded_image():
    with open("PHOTO.jpg", "rb") as img_file:
        my_string = base64.b64encode(img_file.read())
    print(my_string)

    return (my_string)


encoded_image = get_base64_encoded_image()
decodeit = open('hello_level.jpg', 'wb')
decodeit.write(base64.b64decode((encoded_image)))
decodeit.close()

