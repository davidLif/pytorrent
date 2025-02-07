__author__ = 'dl591'

import time
import logging

import peers_manager
import pieces_manager
import torrent
import tracker
from block import State
import message


class SingleTorrentRunner(object):
    percentage_completed = -1
    last_log_line = ""

    def __init__(self, logger, torrent_path):
        self.__logger = logger
        self.__seed_file_forever = True

        try:
            self.torrent = torrent.Torrent().load_from_path(torrent_path)
            self.tracker = tracker.Tracker(self.torrent)

            self.pieces_manager = pieces_manager.PiecesManager(self.torrent)
            self.peers_manager = peers_manager.PeersManager(self.torrent, self.pieces_manager, self.tracker)
            self.tracker.set_pieces_manager(self.pieces_manager)
        except Exception as e:
            self.__logger.error("Failed to load torrent_3.torrent", exc_info=e)
            exit(-1)

    def start(self):
        self.peers_manager.start()
        self.__logger.info("PeersManager Started")
        self.__logger.info("PiecesManager Started")

        peers_dict = self.tracker.get_peers_from_trackers()
        self.peers_manager.add_peers(peers_dict.values())

        while not self.pieces_manager.all_pieces_completed():
            if not self.peers_manager.has_unchoked_peers():
                time.sleep(1)
                logging.info("No unchocked peers")
                continue

            for piece in self.pieces_manager.pieces:
                index = piece.piece_index

                if self.pieces_manager.pieces[index].is_full:
                    continue

                peer = self.peers_manager.get_random_peer_having_piece(index)
                if not peer:
                    continue

                self.pieces_manager.pieces[index].update_block_status()

                data = self.pieces_manager.pieces[index].get_empty_block()
                if not data:
                    continue

                piece_index, block_offset, block_length = data
                piece_data = message.Request(piece_index, block_offset, block_length).to_bytes()
                logging.debug("Sent " + str(message.Request(piece_index, block_offset, block_length)))
                peer.send_to_peer(piece_data)

            self.display_progression()

            time.sleep(0.1)

        logging.info("File(s) downloaded successfully.")

        self.display_progression()

        # Exit program or continue seeding the file
        try:
            while self.__seed_file_forever:
                time.sleep(5)
                peers_dict = self.tracker.get_peers_from_trackers()
                self.peers_manager.add_peers(peers_dict.values())
        except KeyboardInterrupt:
            pass  # Shutdown request
        finally:
            self._exit_threads()

    def start_seed_only(self):
        self.peers_manager.start()
        self.__logger.info("PeersManager Started")
        self.__logger.info("PiecesManager Started")

        while True:
            time.sleep(10000)

    def display_progression(self):
        new_progression = 0

        for i in range(self.pieces_manager.number_of_pieces):
            for j in range(self.pieces_manager.pieces[i].number_of_blocks):
                if self.pieces_manager.pieces[i].blocks[j].state == State.FULL:
                    new_progression += len(self.pieces_manager.pieces[i].blocks[j].data)

        if new_progression == self.percentage_completed:
            return

        number_of_peers = self.peers_manager.unchoked_peers_count()
        percentage_completed = float((float(new_progression) / self.torrent.total_length) * 100)

        current_log_line = "Connected peers: {} - {}% completed | {}/{} pieces".format(number_of_peers,
                                                                                         round(percentage_completed, 2),
                                                                                         self.pieces_manager.complete_pieces,
                                                                                         self.pieces_manager.number_of_pieces)
        if current_log_line != self.last_log_line:
            logging.debug(current_log_line)

        self.last_log_line = current_log_line
        self.percentage_completed = new_progression

    def _exit_threads(self):
        self.peers_manager.is_active = False
        exit(0)
