import threading
import time

__author__ = 'dl591'

import socket
import struct
import bitstring
from pubsub import pub
import logging

import message
from bitstring import BitArray


class Peer(object):
    def __init__(self, related_torrent_obj, torrent_pieces_array, ip, port=6881):
        self.last_call = 0.0
        self.has_handshaked = False
        self.healthy = False
        self.read_buffer = b''
        self.socket = None
        self.socket_lock = threading.Lock()
        self.ip = ip
        self.port = port
        self.torrent_pieces_array = torrent_pieces_array
        self.related_torrent_obj = related_torrent_obj
        self.number_of_pieces = int(related_torrent_obj.number_of_pieces)
        self.bit_field = bitstring.BitArray(self.number_of_pieces)
        self.state = {
            'am_choking': True,
            'am_interested': False,
            'peer_choking': True,
            'peer_interested': False,
        }

    def __hash__(self):
        return "%s:%d" % (self.ip, self.port)

    def connect(self):
        try:
            self.socket = socket.create_connection((self.ip, self.port), timeout=2)
            self.socket.setblocking(False)
            logging.debug("Connected to peer ip: {} - port: {}".format(self.ip, self.port))
            self.healthy = True

        except Exception as e:
            logging.warning("Failed to connect to peer (ip: %s - port: %s - %s)" % (self.ip, self.port, e.__str__()))
            return False

        return True

    def send_to_peer(self, msg):
        # TODO: Handle socket overload - better!
        try:
            self.socket_lock.acquire()
            if time.time() - self.last_call < 0.01:
                time.sleep(0.01)
            self.socket.send(msg)
            self.last_call = time.time()
        except Exception as e:
            self.healthy = False
            logging.error("Failed to send to peer : %s" % e.__str__(), exc_info=e)
        finally:
            self.socket_lock.release()

    def is_eligible(self):
        now = time.time()
        return (now - self.last_call) > 0.2

    def has_piece(self, index):
        return self.bit_field[index]

    def am_choking(self):
        return self.state['am_choking']

    def am_unchoking(self):
        return not self.am_choking()

    def is_choking(self):
        return self.state['peer_choking']

    def is_unchoked(self):
        return not self.is_choking()

    def is_interested(self):
        return self.state['peer_interested']

    def am_interested(self):
        return self.state['am_interested']

    def handle_choke(self):
        logging.debug('handle_choke - %s' % self.ip)
        self.state['peer_choking'] = True

    def handle_unchoke(self):
        logging.debug('handle_unchoke - %s' % self.ip)
        self.state['peer_choking'] = False

    def handle_interested(self):
        logging.debug('handle_interested - %s' % self.ip)
        self.state['peer_interested'] = True

        if self.am_choking():
            self.state['am_choking'] = False
            unchoke = message.UnChoke().to_bytes()
            logging.debug("Sent " + str(message.UnChoke()))
            self.send_to_peer(unchoke)

    def handle_not_interested(self):
        logging.debug('handle_not_interested - %s' % self.ip)
        self.state['peer_interested'] = False

    def handle_have(self, have):
        """
        :type have: message.Have
        """
        logging.debug('handle_have - ip: %s - piece: %s' % (self.ip, have.piece_index))
        self.bit_field[have.piece_index] = True

        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            logging.debug("Sent " + str(message.Interested()))
            self.send_to_peer(interested)
            self.state['am_interested'] = True

        # pub.sendMessage('RarestPiece.updatePeersBitfield', bitfield=self.bit_field)

    def handle_bitfield(self, bitfield):
        """
        :type bitfield: message.BitField
        """
        logging.debug('handle_bitfield - %s - %s' % (self.ip, bitfield.bitfield))
        self.bit_field = bitfield.bitfield

        if self.is_choking() and not self.state['am_interested']:
            interested = message.Interested().to_bytes()
            logging.debug("Sent " + str(message.Interested()))
            self.send_to_peer(interested)
            self.state['am_interested'] = True

        # pub.sendMessage('RarestPiece.updatePeersBitfield', bitfield=self.bit_field)

    def handle_request(self, request):
        """
        :type request: message.Request
        """
        logging.debug('handle_request - %s' % self.ip)
        if self.is_interested() and self.am_unchoking():
            pub.sendMessage('PeersManager.PeerRequestsPiece', request=request, peer=self)

    def handle_piece(self, message):
        """
        :type message: message.Piece
        """
        pub.sendMessage('PiecesManager.Piece', piece=(message.piece_index, message.block_offset, message.block))

    def handle_cancel(self):
        logging.debug('handle_cancel - %s' % self.ip)

    def handle_port_request(self):
        logging.debug('handle_port_request - %s' % self.ip)

    def _handle_handshake(self):
        try:
            handshake_message = message.Handshake.from_bytes(self.read_buffer)
            self.has_handshaked = True
            self.read_buffer = self.read_buffer[handshake_message.total_length:]
            logging.debug('handle_handshake - %s' % self.ip)

            if handshake_message.info_hash != self.related_torrent_obj.info_hash:
                raise AssertionError("Handshake info hash is different then"
                                     " the related torrent object info hash. {} {}".format(
                    str(handshake_message.info_hash), str(self.related_torrent_obj.info_hash)))

            bit_field_array = BitArray(len(self.torrent_pieces_array))
            for piece_index in range(len(self.torrent_pieces_array)):
                piece = self.torrent_pieces_array[piece_index]
                bit_field_array[piece_index] = piece.is_full
            self.send_to_peer(message.BitField(bit_field_array).to_bytes())
            logging.debug("Sent " + str(message.BitField(bit_field_array)))

            return True

        except Exception:
            logging.exception("First message should always be a handshake message")
            self.healthy = False

        return False

    def _handle_keep_alive(self):
        try:
            keep_alive = message.KeepAlive.from_bytes(self.read_buffer)
            logging.debug('handle_keep_alive - %s' % self.ip)
        except message.WrongMessageException:
            return False
        except Exception:
            logging.exception("Error KeepALive, (need at least 4 bytes : {})".format(len(self.read_buffer)))
            return False

        self.read_buffer = self.read_buffer[keep_alive.total_length:]
        return True

    def handle_incoming_data(self):
        for message in self.get_messages():
            logging.debug("Received " + str(message))
            self.process_new_message(message)

    def get_messages(self):
        while len(self.read_buffer) > 4 and self.healthy:
            if (not self.has_handshaked and self._handle_handshake()) or self._handle_keep_alive():
                continue

            payload_length, = struct.unpack(">I", self.read_buffer[:4])
            total_length = payload_length + 4

            if len(self.read_buffer) < total_length:
                break
            else:
                payload = self.read_buffer[:total_length]
                self.read_buffer = self.read_buffer[total_length:]

            try:
                received_message = message.MessageDispatcher(payload).dispatch()
                if received_message:
                    yield received_message
            except message.WrongMessageException as e:
                logging.exception(e.__str__())

    def process_new_message(self, new_message: message.Message):
        if isinstance(new_message, message.Handshake) or isinstance(new_message, message.KeepAlive):
            logging.error("Handshake or KeepALive should have already been handled")

        elif isinstance(new_message, message.Choke):
            self.handle_choke()

        elif isinstance(new_message, message.UnChoke):
            self.handle_unchoke()

        elif isinstance(new_message, message.Interested):
            self.handle_interested()

        elif isinstance(new_message, message.NotInterested):
            self.handle_not_interested()

        elif isinstance(new_message, message.Have):
            self.handle_have(new_message)

        elif isinstance(new_message, message.BitField):
            self.handle_bitfield(new_message)

        elif isinstance(new_message, message.Request):
            self.handle_request(new_message)

        elif isinstance(new_message, message.Piece):
            self.handle_piece(new_message)

        elif isinstance(new_message, message.Cancel):
            self.handle_cancel()

        elif isinstance(new_message, message.Port):
            self.handle_port_request()

        else:
            logging.error("Unknown message")
