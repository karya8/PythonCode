import socket
import requests

def get_api_ip_address(api_url):
    """
    Finds the IP address of an API server.

    Args:
        api_url (str): The URL of the API endpoint (e.g., "https://api.example.com/data").

    Returns:
        str: The IP address of the API server, or None if it cannot be determined.
    """
    try:
        # 1. Extract the hostname from the URL
        from urllib.parse import urlparse
        parsed_url = urlparse(api_url)
        hostname = parsed_url.hostname

        if not hostname:
            print(f"Error: Could not extract hostname from URL: {api_url}")
            return None

        # 2. Use socket.gethostbyname to resolve the hostname to an IP address
        ip_address = socket.gethostbyname(hostname)
        return ip_address
    except socket.gaierror as e:
        print(f"Error resolving hostname {hostname}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# Example usage:
api_endpoint = "https://jsonplaceholder.typicode.com/todos/1"
ip = get_api_ip_address(api_endpoint)

if ip:
    print(f"The IP address of {api_endpoint} is: {ip}")
else:
    print(f"Could not determine the IP address for {api_endpoint}.")

# You can also use the 'requests' library to make the API call and
# sometimes get the peer information directly from the response object,
# though it's less direct for just getting the IP.
# This approach might be more complex and less reliable for just the IP.
try:
    response = requests.get(api_endpoint, stream=True)
    if response.raw._connection and hasattr(response.raw._connection, 'sock') and hasattr(response.raw._connection.sock, 'getpeername'):
        peer_address = response.raw._connection.sock.getpeername()
        print(f"IP address from requests response (peername): {peer_address[0]}")
    else:
        print("Could not get peer address from requests response.")
except requests.exceptions.RequestException as e:
    print(f"Error making API request with requests: {e}")

e
