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
    Behandelt doppelte Chat-IDs, indem die Nachrichten zusammengeführt werden.
    
    Args:
        json_file_path (str): Pfad zur JSON-Datei
        h5_file_path (str): Pfad, wo die H5-Datei gespeichert werden soll
    """
    print(f"Lese JSON-Datei: {json_file_path}")
    
    # JSON-Datei einlesen
    with open(json_file_path, 'r', encoding='utf-8') as file:
        chat_data = json.load(file)
    
    print(f"Gefundene Chats: {len(chat_data)}")
    
    # Sammle zuerst alle Chats nach IDs, um Duplikate zu erkennen und zusammenzuführen
    chat_dict = {}
    
    for chat in chat_data:
        chat_id = chat['chat_id']
        
        if chat_id in chat_dict:
            # Dieser Chat wurde bereits gesehen - füge die Nachrichten zusammen
            print(f"Duplikat gefunden für Chat-ID: {chat_id} - füge Nachrichten zusammen")
            existing_chat = chat_dict[chat_id]
            
            # Aktualisiere Metadaten
            total_messages = existing_chat['message_count'] + chat['message_count']
            existing_chat['message_count'] = total_messages
            
            # Sammle alle eindeutigen Sender
            all_senders = set()
            for msg in existing_chat['messages']:
                if 'sender_alias' in msg:
                    all_senders.add(msg['sender_alias'])
            for msg in chat['messages']:
                if 'sender_alias' in msg:
                    all_senders.add(msg['sender_alias'])
            
            existing_chat['unique_sender_count'] = len(all_senders)
            
            # Füge die neuen Nachrichten hinzu
            existing_chat['messages'].extend(chat['messages'])
            
            # Sortiere Nachrichten nach Zeitstempel
            try:
                existing_chat['messages'].sort(key=lambda msg: datetime.strptime(msg.get('timestamp', '1900-01-01 00:00:00'), 
                                                                             "%Y-%m-%d %H:%M:%S"))
            except:
                print(f"  Fehler beim Sortieren der Nachrichten für Chat {chat_id}")
        else:
            # Neuer Chat
            chat_dict[chat_id] = chat
    
    print(f"Eindeutige Chats nach Duplikatentfernung: {len(chat_dict)}")
    
    # H5-Datei erstellen
    with h5py.File(h5_file_path, 'w') as hf:
        # Durchlaufe jeden Chat (jetzt ohne Duplikate)
        for chat_idx, (chat_id, chat) in enumerate(chat_dict.items()):
            print(f"Verarbeite Chat {chat_idx+1}/{len(chat_dict)}: {chat_id}")
            
            # Erstelle einen gültigen HDF5-Gruppenname, indem ungültige Zeichen ersetzt werden
            safe_chat_id = chat_id
            if ":" in chat_id:
                # HDF5 Pfade können keine Doppelpunkte enthalten, ersetzen durch Unterstrich
                safe_chat_id = chat_id.replace(":", "_")
                print(f"  Originale Chat-ID enthält unerlaubte Zeichen, verwende sicheren Namen: {safe_chat_id}")
            
            # Gruppe für diesen Chat erstellen
            chat_group = hf.create_group(safe_chat_id)
            
            # Speichere die originale Chat-ID auch als Attribut
            chat_group.attrs['original_chat_id'] = chat_id
            
            # Chat-Metadaten als Attribute hinzufügen
            chat_group.attrs['unique_sender_count'] = chat['unique_sender_count']
            chat_group.attrs['message_count'] = chat['message_count']
            
            # Chat-Namen extrahieren und bereinigen
            if 'chat_name' in chat:
                chat_name = chat['chat_name']
            else:
                # Ableiten eines vernünftigen Chat-Namens aus der Chat-ID
                parts = chat_id.split(':')
                if len(parts) > 1 and parts[0].startswith('!'):
                    chat_name = parts[-1]  # Verwende die Domain als Chat-Namen
                else:
                    chat_name = chat_id
            
            chat_group.attrs['chat_name'] = chat_name
            
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
                message_m2m100_texts = []
                has_deepl = False
                has_m2m100 = False
                
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

                        # Übersetzte Nachricht (wenn vorhanden)
                    if 'message_m2m100' in msg:
                        has_m2m100 = True
                        message_m2m100_texts.append(msg['message_m2m100'])
                    else:
                        message_m2m100_texts.append('')
                
                # Erstelle Datasets für die Nachrichtendaten
                dt_string = h5py.special_dtype(vlen=str)
                
                timestamps_dataset = chat_group.create_dataset('timestamp', data=timestamps)
                timestamp_str_dataset = chat_group.create_dataset('timestamp_str', data=timestamp_strings, dtype=dt_string)
                
                sender_dataset = chat_group.create_dataset('sender_alias', data=sender_aliases, dtype=dt_string)
                message_dataset = chat_group.create_dataset('message', data=message_texts, dtype=dt_string)
                id_dataset = chat_group.create_dataset('message_id', data=message_ids)
                
                if has_deepl:
                    deepl_dataset = chat_group.create_dataset('message_deepl', data=message_deepl_texts, dtype=dt_string)

                if has_m2m100:
                    m2m100_dataset = chat_group.create_dataset('message_m2m100', data=message_m2m100_texts, dtype=dt_string)
    
    print(f"Konvertierung abgeschlossen. H5-Datei gespeichert unter: {h5_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Konvertiert eine JSON-Datei mit Chat-Daten in eine H5-Datei.")
    parser.add_argument("json_file", help="Pfad zur JSON-Datei")
    parser.add_argument("--output", "-o", help="Pfad zur Ausgabe-H5-Datei (Optional)", default=None)
    parser.add_argument("--overwrite", "-w", action="store_true", help="Überschreibe die Ausgabedatei, falls sie existiert")
    
    args = parser.parse_args()
    
    json_file_path = args.json_file
    
    if args.output:
        h5_file_path = args.output
    else:
        # Wenn kein Ausgabepfad angegeben ist, nutze den gleichen Namen wie die JSON-Datei
        h5_file_path = os.path.splitext(json_file_path)[0] + '.h5'
    
    # Prüfe, ob die Ausgabedatei bereits existiert
    if os.path.exists(h5_file_path) and not args.overwrite:
        print(f"Die Ausgabedatei {h5_file_path} existiert bereits. Verwende --overwrite, um sie zu überschreiben.")
        exit(1)
    
    convert_json_to_h5(json_file_path, h5_file_path)