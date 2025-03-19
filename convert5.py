import json
import h5py
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import os

def convert_json_to_h5(json_file_path, h5_file_path):
    """
    Konvertiert eine JSON-Datei mit Chat-Daten in eine H5-Datei.
    
    Args:
        json_file_path (str): Pfad zur JSON-Datei
        h5_file_path (str): Pfad, wo die H5-Datei gespeichert werden soll
    """
    print(f"Lese JSON-Datei: {json_file_path}")
    
    # JSON-Datei einlesen
    with open(json_file_path, 'r', encoding='utf-8') as file:
        chat_data = json.load(file)
    
    print(f"Gefundene Chats: {len(chat_data)}")
    
    # H5-Datei erstellen
    with h5py.File(h5_file_path, 'w') as hf:
        # Durchlaufe jeden Chat
        for chat_idx, chat in enumerate(chat_data):
            chat_id = chat['chat_id']
            print(f"Verarbeite Chat {chat_idx+1}/{len(chat_data)}: {chat_id}")
            
            # Gruppe für diesen Chat erstellen
            chat_group = hf.create_group(chat_id)
            
            # Chat-Metadaten als Attribute hinzufügen
            chat_group.attrs['unique_sender_count'] = chat['unique_sender_count']
            chat_group.attrs['message_count'] = chat['message_count']
            chat_group.attrs['chat_name'] = chat_id.split(':')[-1] if ':' in chat_id else chat_id
            
            # Extrahiere Nachrichtendaten
            messages = chat['messages']
            if messages:
                # Datenstrukturen für H5-Datasets vorbereiten
                timestamps = []
                timestamp_strings = []
                sender_aliases = []
                message_texts = []
                message_ids = []
                message_deepl_texts = []
                has_deepl = False
                
                # Gehe durch jede Nachricht im Chat
                for msg in messages:
                    # Zeitstempel als Unix-Timestamp und als String speichern
                    try:
                        dt = datetime.strptime(msg['timestamp'], "%Y-%m-%d %H:%M:%S")
                        timestamps.append(dt.timestamp())
                        timestamp_strings.append(msg['timestamp'])
                    except:
                        timestamps.append(np.nan)
                        timestamp_strings.append(msg.get('timestamp', ''))
                    
                    # Sender und Nachrichtentext
                    sender_aliases.append(msg.get('sender_alias', ''))
                    message_texts.append(msg.get('message', ''))
                    message_ids.append(msg.get('message_id', -1))
                    
                    # Übersetzte Nachricht (wenn vorhanden)
                    if 'message_deepl' in msg:
                        has_deepl = True
                        message_deepl_texts.append(msg['message_deepl'])
                    else:
                        message_deepl_texts.append('')
                
                # Erstelle Datasets für die Nachrichtendaten
                dt_string = h5py.special_dtype(vlen=str)
                
                timestamps_dataset = chat_group.create_dataset('timestamp', data=timestamps)
                timestamp_str_dataset = chat_group.create_dataset('timestamp_str', data=timestamp_strings, dtype=dt_string)
                
                sender_dataset = chat_group.create_dataset('sender_alias', data=sender_aliases, dtype=dt_string)
                message_dataset = chat_group.create_dataset('message', data=message_texts, dtype=dt_string)
                id_dataset = chat_group.create_dataset('message_id', data=message_ids)
                
                if has_deepl:
                    deepl_dataset = chat_group.create_dataset('message_deepl', data=message_deepl_texts, dtype=dt_string)
    
    print(f"Konvertierung abgeschlossen. H5-Datei gespeichert unter: {h5_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Konvertiert eine JSON-Datei mit Chat-Daten in eine H5-Datei.")
    parser.add_argument("json_file", help="Pfad zur JSON-Datei")
    parser.add_argument("--output", "-o", help="Pfad zur Ausgabe-H5-Datei (Optional)", default=None)
    
    args = parser.parse_args()
    
    json_file_path = args.json_file
    
    if args.output:
        h5_file_path = args.output
    else:
        # Wenn kein Ausgabepfad angegeben ist, nutze den gleichen Namen wie die JSON-Datei
        h5_file_path = os.path.splitext(json_file_path)[0] + '.h5'
    
    convert_json_to_h5(json_file_path, h5_file_path)