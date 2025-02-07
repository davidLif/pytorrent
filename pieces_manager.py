__author__ = 'alexisgallepe'

import os
import os.path

import piece
import bitstring
import logging
from pubsub import pub


class PiecesManager(object):
    def __init__(self, torrent):
        self.torrent = torrent
        self.number_of_pieces = int(torrent.number_of_pieces)
        self.bitfield = bitstring.BitArray(self.number_of_pieces)
        self.pieces = self._generate_pieces()
        self.files = self._load_files()
        self.complete_pieces = 0

        for file in self.files:
            id_piece = file['idPiece']
            self.pieces[id_piece].files.append(file)

        self.try_load_file_from_storage()

        # events
        pub.subscribe(self.receive_block_piece, 'PiecesManager.Piece')
        pub.subscribe(self.update_bitfield, 'PiecesManager.PieceCompleted')

    def update_bitfield(self, piece_index):
        self.bitfield[piece_index] = 1

    def receive_block_piece(self, piece):
        piece_index, piece_offset, piece_data = piece

        if self.pieces[piece_index].is_full:
            return

        self.pieces[piece_index].set_block(piece_offset, piece_data)

        if self.pieces[piece_index].are_all_blocks_full():
            if self.pieces[piece_index].set_to_full():
                self.complete_pieces += 1

    def receive_full_piece(self, piece):
        piece_index, _, piece_data = piece

        if self.pieces[piece_index].is_full:
            return

        self.pieces[piece_index].set_full_piece(piece_data)

        if self.pieces[piece_index].are_all_blocks_full():
            if self.pieces[piece_index].set_to_full():
                self.complete_pieces += 1


    def get_block(self, piece_index, block_offset, block_length):
        for piece in self.pieces:
            if piece_index == piece.piece_index:
                if piece.is_full:
                    return piece.get_block(block_offset, block_length)
                else:
                    break

        return None

    def all_pieces_completed(self):
        for piece in self.pieces:
            if not piece.is_full:
                return False

        return True

    def _generate_pieces(self):
        pieces = []
        last_piece = self.number_of_pieces - 1

        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20

            if i == last_piece:
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
                pieces.append(piece.Piece(i, piece_length, self.torrent.pieces[start:end]))
            else:
                pieces.append(piece.Piece(i, self.torrent.piece_length, self.torrent.pieces[start:end]))

        return pieces

    def _load_files(self):
        files = []
        piece_offset = 0
        piece_size_used = 0

        for f in self.torrent.file_names:
            current_size_file = f["length"]
            file_offset = 0

            while current_size_file > 0:
                id_piece = int(piece_offset / self.torrent.piece_length)
                piece_size = self.pieces[id_piece].piece_size - piece_size_used

                if current_size_file - piece_size < 0:
                    file = {"length": current_size_file,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    piece_offset += current_size_file
                    file_offset += current_size_file
                    piece_size_used += current_size_file
                    current_size_file = 0

                else:
                    current_size_file -= piece_size
                    file = {"length": piece_size,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    piece_offset += piece_size
                    file_offset += piece_size
                    piece_size_used = 0

                files.append(file)
        return files

    def try_load_file_from_storage(self):
        file_reader = None
        for piece_for_file in self.files:
            file_path = os.path.join("torrent_files_and_data", piece_for_file["path"])
            file_index = piece_for_file["fileOffset"]

            if os.path.exists(file_path):
                if not file_reader:
                    file_reader = open(file_path, "rb")
                file_reader.seek(file_index)

                piece_data = file_reader.read(piece_for_file["length"])
                self.receive_full_piece((piece_for_file["idPiece"], piece_for_file["pieceOffset"], piece_data))
        if file_reader:
            file_reader.close()


