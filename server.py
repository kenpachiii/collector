import os
import sys
import json
import socket # For gethostbyaddr()
import urllib.parse
import posixpath

from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer, BaseHTTPRequestHandler
from http import HTTPStatus
from store import Store

class Server(BaseHTTPRequestHandler):

    def __init__(self, *args, directory=None, **kwargs):
        if directory is None:
            directory = os.getcwd()
        self.directory = os.fspath(directory)
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Serve a GET request."""
        store = self.send_head()
        if store:
            try:

                since = 0

                url = urllib.parse.urlparse(self.path)
                if url.query:
                    query = url.query.split(',')

                    for q in query:
                        key, value = q.split('=')

                        if key == 'since':
                            since = value

                trades = store.fetch_trades(since)
                self.wfile.write(json.dumps(trades).encode())

            finally:
                store.close()

    def send_head(self):
        path = self.translate_path(self.path)

        if not os.path.exists(os.path.join(path, 'store.db')):
            self.send_error(HTTPStatus.NOT_FOUND, 'Symbol not found')
            return None

        store: Store = Store(os.path.join(path, 'store.db'))

        try:
            self.send_response(HTTPStatus.OK)
            self.end_headers()
            return store
        except:
            store.close()
            raise

    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        # abandon query parameters
        path = path.split('?',1)[0]
        path = path.split('#',1)[0]
        # Don't forget explicit trailing slash when normalizing. Issue17324
        trailing_slash = path.rstrip().endswith('/')
        try:
            path = urllib.parse.unquote(path, errors='surrogatepass')
        except UnicodeDecodeError:
            path = urllib.parse.unquote(path)
        path = posixpath.normpath(path)
        words = path.split('/')
        words = filter(None, words)
        path = self.directory
        for word in words:
            if os.path.dirname(word) or word in (os.curdir, os.pardir):
                # Ignore components that are not a simple file/directory name
                continue
            path = os.path.join(path, word)
        if trailing_slash:
            path += '/'
        return path

def _get_best_family(*address):
    infos = socket.getaddrinfo(
        *address,
        type=socket.SOCK_STREAM,
        flags=socket.AI_PASSIVE,
    )
    family, type, proto, canonname, sockaddr = next(iter(infos))
    return family, sockaddr

def test(HandlerClass=BaseHTTPRequestHandler,
         ServerClass=ThreadingHTTPServer,
         protocol='HTTP/1.0', port=8000, bind=None):
    '''Test the HTTP request handler class.

    This runs an HTTP server on port 8000 (or the port argument).

    '''
    ServerClass.address_family, addr = _get_best_family(bind, port)
    HandlerClass.protocol_version = protocol
    with ServerClass(addr, HandlerClass) as httpd:
        host, port = httpd.socket.getsockname()[:2]
        url_host = f'[{host}]' if ':' in host else host
        print(
            f'Serving HTTP on {host} port {port} '
            f'(http://{url_host}:{port}/) ...'
        )
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nKeyboard interrupt received, exiting.')
            sys.exit(0)

if __name__ == '__main__':
    import argparse
    import contextlib

    parser = argparse.ArgumentParser()
    parser.add_argument('--bind', '-b', metavar='ADDRESS',
                        help='specify alternate bind address '
                             '(default: all interfaces)')
    parser.add_argument('--directory', '-d', default=os.getcwd(),
                        help='specify alternate directory '
                             '(default: current directory)')
    parser.add_argument('port', action='store', default=8000, type=int,
                        nargs='?',
                        help='specify alternate port (default: 8000)')
    args = parser.parse_args()
    handler_class = Server

    # ensure dual-stack is not disabled; ref #38907
    class DualStackServer(ThreadingHTTPServer):

        def server_bind(self):
            # suppress exception when protocol is IPv4
            with contextlib.suppress(Exception):
                self.socket.setsockopt(
                    socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            return super().server_bind()

        def finish_request(self, request, client_address):
            self.RequestHandlerClass(request, client_address, self,
                                     directory=args.directory)

    test(
        HandlerClass=handler_class,
        ServerClass=DualStackServer,
        port=args.port,
        bind=args.bind,
    )