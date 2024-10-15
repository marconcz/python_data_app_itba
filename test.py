from requests import post, get

# Twitch Dev Secrets
user = '2qrwbkgp87w3x3t4eygc462tr3b1xx'  # Client-ID
secret = 'rtmybybdolmc85bhk0ougu21bmsek0'  # Client Secret

# Obtener el token de acceso
token_response = post(f'https://id.twitch.tv/oauth2/token?client_id={user}&client_secret={secret}&grant_type=client_credentials')
token = token_response.json().get('access_token')

# URL para la API de IGDB
url = "https://api.igdb.com/v4/games"

# Realizar la solicitud a la API de IGDB
response = post(
    url,
    headers={
        'Client-ID': user,
        'Authorization': f'Bearer {token}'
    },
    data='fields *; limit 1;'
)

# Imprimir cada elemento del JSON
if response.status_code == 200:
    json_data = response.json()  # Obtener el JSON como lista de diccionarios
    for index, game in enumerate(json_data):
        print(f"Juego {index + 1}:")
        for key, value in game.items():  # Iterar sobre cada clave y valor del diccionario
            print(f"  {key}: {value}")
        print()  # Línea en blanco para separar juegos
else:
    print(f"Error: {response.status_code} - {response.text}")
