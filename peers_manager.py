import time

__author__ = 'alexisgallepe'

import select
from threading import Thread
from pubsub import pub
import rarest_piece
import logging
import message
import peer
import errno
import socket
import random


class PeersManager(Thread):
    def __init__(self, torrent, pieces_manager, peer_tracker):
        Thread.__init__(self)
        self.peers = []
        self.torrent = torrent
        self.pieces_manager = pieces_manager
        self.peer_tracker = peer_tracker
        self.rarest_pieces = rarest_piece.RarestPieces(pieces_manager)
        self.pieces_by_peer = [[0, []] for _ in range(pieces_manager.number_of_pieces)]
        self.is_active = True

        # Events
        pub.subscribe(self.peer_requests_piece, 'PeersManager.PeerRequestsPiece')
        pub.subscribe(self.peers_bitfield, 'PeersManager.updatePeersBitfield')

    def peer_requests_piece(self, request=None, peer=None):
        if not request or not peer:
            logging.error("empty request/peer message")

        piece_index, block_offset, block_length = request.piece_index, request.block_offset, request.block_length

        logging.info("Peer requested piece index {}, block offset {}, block length {}. peer : {}".format(
            piece_index, block_offset, block_length, peer.ip))

        block = self.pieces_manager.get_block(piece_index, block_offset, block_length)
        if block:
            piece = message.Piece(block_length, piece_index, block_offset, block).to_bytes()
            logging.debug("Sent " + str(message.Piece(block_length, piece_index, block_offset, block)))
            peer.send_to_peer(piece)
            logging.info("Sent piece index {} to peer : {}".format(request.piece_index, peer.ip))

    def peers_bitfield(self, bitfield=None):
        for i in range(len(self.pieces_by_peer)):
            if bitfield[i] == 1 and peer not in self.pieces_by_peer[i][1] and self.pieces_by_peer[i][0]:
                self.pieces_by_peer[i][1].append(peer)
                self.pieces_by_peer[i][0] = len(self.pieces_by_peer[i][1])

    def get_random_peer_having_piece(self, index):
        ready_peers = []

        for peer in self.peers:
            if peer.is_eligible() and peer.is_unchoked() and peer.am_interested() and peer.has_piece(index):
                ready_peers.append(peer)

        return random.choice(ready_peers) if ready_peers else None

    def has_unchoked_peers(self):
        for peer in self.peers:
            if peer.is_unchoked():
                return True
        return False

    def unchoked_peers_count(self):
        cpt = 0
        for peer in self.peers:
            if peer.is_unchoked():
                cpt += 1
        return cpt


    @staticmethod
    def _read_from_socket(sock):
        data = b''

        while True:
            try:
                buff = sock.recv(4096)
                if len(buff) <= 0:
                    break

                data += buff
            except socket.error as e:
                err = e.args[0]
                if err != errno.EAGAIN or err != errno.EWOULDBLOCK:
                    logging.error("Wrong errno {}".format(err))
                break
            except Exception:
                logging.exception("Recv failed")
                break

        return data

    def run(self):
        while self.is_active:
            read = [peer.socket for peer in self.peers]
            read_list, _, _ = select.select(read, [], [], 1)

            for socket in read_list:
                peer = self.get_peer_by_socket(socket)
                if not peer.healthy:
                    self.remove_peer(peer)
                    continue

                try:
                    payload = self._read_from_socket(socket)
                except Exception as e:
                    logging.error("Recv failed %s" % e.__str__())
                    self.remove_peer(peer)
                    continue

                peer.read_buffer += payload
                logging.debug(peer.read_buffer)

                peer.handle_incoming_data()

    def _do_handshake(self, peer):
        try:
            handshake = message.Handshake(self.torrent.info_hash)
            logging.debug("Sent " + str(handshake))
            peer.send_to_peer(handshake.to_bytes())
            logging.info("new peer added : %s" % peer.ip)
            return True

        except Exception:
            logging.exception("Error when sending Handshake message")

        return False

    def add_peers(self, peers):
        for peer in peers:
            if self._do_handshake(peer):
                self.peers.append(peer)
            else:
                logging.error("Error _do_handshake")

    def remove_peer(self, peer):
        if peer in self.peers:
            try:
                peer.socket.close()
            except Exception:
                logging.exception("Falied to close connection on removed peer {}".format(str(peer.ip)))

            self.peers.remove(peer)
            self.peer_tracker.remove_disconnected_peer(peer)

        #for rarest_piece in self.rarest_pieces.rarest_pieces:
        #    if peer in rarest_piece["peers"]:
        #        rarest_piece["peers"].remove(peer)

    def get_peer_by_socket(self, socket):
        for peer in self.peers:
            if socket == peer.socket:
                return peer

        raise Exception("Peer not present in peer_list")


