# pytorrent
Simple and functional BitTorrent client made in Python. 
This is a fork from https://github.com/gallexis/pytorrent. I fixed some of the original bugs, and changed some of the code structure.

Usage:
`runner = SingleTorrentRunner(logger, torent-file-path)
runner.start` 

A SingleTorrentRunner object handles both seeding a leaching of the it's input torrent file
