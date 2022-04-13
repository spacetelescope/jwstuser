from getpass import getpass
from pathlib import Path
from os import getenv

def get_mast_api_token(mast_api_token=None):
    '''Get MAST API token. Precedence is arg, env, file, prompt.'''
    if not mast_api_token:
        mast_api_token = getenv('MAST_API_TOKEN')
    if not mast_api_token:
        path = Path.home() / '.mast_api_token'
        try:
            with open(path, 'r') as fp:
                lines = fp.read().splitlines()
                if len(lines) == 1:
                    mast_api_token = lines[0]
                else:
                    print('Ignoring ~/.mast_api_token, expected one line')
        except FileNotFoundError:
            pass
    if not mast_api_token and prompt:
        mast_api_token = getpass('Enter MAST API token: ')
    try:
        return verified_mast_api_token(mast_api_token)
    except ValueError as e:
        raise e

def verified_mast_api_token(token):
    '''Return input MAST API token, if type, length, characters are valid.'''
    is_not = 'MAST API token is not'
    if token:
        if type(token) is str:
            if len(token) == 32:
                if token.isalnum():
                    return token
                else:
                    raise ValueError(f"{is_not} alphanumeric: '{token}'")
            else:
                raise ValueError(f"{is_not} 32 characters: '{token}'")
        else:
            raise ValueError(f"{is_not} type str: {type(token)}")
    else:
        raise ValueError(f"{is_not} defined")
