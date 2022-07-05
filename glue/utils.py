import json

def parse_json(json_val, key:str, separator:str=None, idx:int=None):
    if not json_val:
        return None

    dic_val = json.loads(json_val)
    if isinstance(dic_val, list):
        dic_val = dic_val[0]

    if not separator:
        try:
            if isinstance(dic_val[key], str):
                return int(dic_val[key])
            return dic_val[key]
        except ValueError:
            return dic_val[key]          
        except KeyError:
            return None
    try:
        return dic_val[key].split(separator)[idx]
    except:
        raise
