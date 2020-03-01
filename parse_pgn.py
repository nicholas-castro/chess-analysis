import chess.pgn
import pandas as pd
from pathlib import Path

games = Path.cwd() / 'data' / 'raw'
processed = Path.cwd() / 'data' / 'processed'

def parse_pgn():
    '''
    Parses pgns in `data/raw` folder and saves outputs (headers and move data) to `data/processed` folder.
    '''
    header_frames = [] # to hold the dataframe for each game before combining
    move_frames = [] # to hold the dataframe for each game before combining
    game_id = 0 # initialize game_id

    for file in games.glob('*.pgn'):

        with open(file) as pgn:
            header_records = {} # adding row data to a dictionary one by one before creating dataframe
            move_records = {} # adding row data to a dictionary one by one before creating dataframe
            i = 0

            while True:

                game = chess.pgn.read_game(pgn)
                if game is None:
                    break

                header_dict = dict(game.headers)
                header_dict['game_id'] = game_id # Add gameid so we can combine data sets
                header_records[i] = header_dict

                j = 0
                moves = list(game.mainline())
                for ix, move in enumerate(moves):
                    move_string = move.san() # save move using standard algebraic notation
                    dot = str(move).find('.') # the move number is always to the left of the period
                    ply_no = int(str(move)[:dot])
                    move_no = ix+1

                    if ix % 2 == 0:
                        mover = 'White'
                    elif ix % 2 == 1:
                        mover = 'Black'

                    move_dict = {'move':move_string, 'ply_no':ply_no, 'move_no':move_no, 'mover':mover, 'game_id':game_id, 'node':move}
                    move_records[j] = move_dict
                    j += 1

                move_df = pd.DataFrame.from_dict(move_records, orient='index')
                move_frames.append(move_df)

                i += 1
                game_id += 1

            header_df = pd.DataFrame.from_dict(header_records, orient='index')
            header_frames.append(header_df)

    headers = pd.concat(header_frames, ignore_index=True).reset_index(drop=True)
    moves = pd.concat(move_frames, ignore_index=True).reset_index(drop=True)

    # saving to disk

    headers_file = Path.cwd() / 'data' / 'processed' / 'headers.h5'
    moves_file = Path.cwd() / 'data' / 'processed' / 'moves.h5'

    headers.to_hdf(headers_file, key='headers')

    # moves can currently only be saved without `node` field
    moves.drop('node', axis=1).to_hdf(moves_file, key='moves')

if __name__=='__main__':
    print('Parsing pgns in `data/raw` folder')
    parse_pgn()
    print('Finished parsing pgns in `data/raw` folder')