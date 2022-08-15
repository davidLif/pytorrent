__author__ = 'dl591'

import logging
from single_torrent_runner import SingleTorrentRunner

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main_logger = logging.getLogger()

    run = SingleTorrentRunner(main_logger, "torrent_files_and_data/pycharm-community-2021.2.2.tar.gz.torrent")
    run.start()
