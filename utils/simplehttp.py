import http.server
import socketserver
import os


def launch_local_server():
    PORT = 8000

    web_dir = "/app"
    os.chdir(web_dir)

    Handler = http.server.SimpleHTTPRequestHandler
    httpd = socketserver.TCPServer(("", PORT), Handler)
    print("serving at port", PORT)
    httpd.serve_forever()


if __name__ == "__main__":
    launch_local_server()
