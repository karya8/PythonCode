import http.server
import socketserver
import os

class MyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # 1. Get the X-Forwarded-For header from the incoming request
        x_forwarded_for = self.headers.get("X-Forwarded-For")

        # 2. Determine the source IP
        if x_forwarded_for:
            # The first IP in the comma-separated list is the original client IP
            source_ip = x_forwarded_for.split(',')[0].strip()
        else:
            # Fallback: If for some reason X-Forwarded-For isn't there,
            # use the direct client address. In Cloud Run, this will be
            # Google's internal load balancer IP, not the user's.
            source_ip = self.client_address[0]

        # 3. Prepare the response
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()

        response_message = f"Hello from Cloud Run!\n"
        response_message += f"Your Source IP Address is: {source_ip}\n"
        response_message += f"(Raw X-Forwarded-For header: {x_forwarded_for})\n"
        response_message += f"(Direct connection IP: {self.client_address[0]})\n"


        self.wfile.write(response_message.encode('utf-8'))

if __name__ == "__main__":
    # Cloud Run provides the port your service should listen on via an environment variable.
    PORT = int(os.environ.get("PORT", 8080))
    # Listen on all network interfaces (required for Cloud Run)
    HOST = "0.0.0.0"

    # Start the simple HTTP server
    with socketserver.TCPServer((HOST, PORT), MyHandler) as httpd:
        print(f"Server listening on {HOST}:{PORT}")
        httpd.serve_forever()

