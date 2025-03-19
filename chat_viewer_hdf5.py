# Anzeigen der Nachrichten für die aktuelle Seite
import os
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import h5py
import numpy as np
import re

# Hilfsfunktion zum Durchsuchen und Anzeigen der H5-Struktur
def explore_h5_structure(hf, path="/", level=0):
    result = []
    indent = "  " * level
    
    # Gruppe und ihre Attribute auflisten
    attrs_str = ""
    if hasattr(hf[path], 'attrs') and len(hf[path].attrs) > 0:
        attrs_str = " [Attributes: " + ", ".join([f"{k}={v}" for k, v in hf[path].attrs.items()]) + "]"
    
    result.append(f"{indent}{path}{attrs_str}")
    
    # Wenn es sich um eine Gruppe handelt, rekursiv die Unterelemente durchgehen
    if isinstance(hf[path], h5py.Group):
        for name in hf[path]:
            child_path = path + name if path == "/" else path + "/" + name
            result.extend(explore_h5_structure(hf, child_path, level+1))
    
    # Wenn es sich um ein Dataset handelt, Details anzeigen
    elif isinstance(hf[path], h5py.Dataset):
        dataset = hf[path]
        shape_str = str(dataset.shape)
        dtype_str = str(dataset.dtype)
        try:
            first_items = str(dataset[:5]) if dataset.shape[0] > 0 else "[]"
            result.append(f"{indent}  [Shape: {shape_str}, Type: {dtype_str}, First items: {first_items}...]")
        except Exception as e:
            result.append(f"{indent}  [Shape: {shape_str}, Type: {dtype_str}, Error accessing data: {str(e)}]")
    
    return result

# Funktion zum Laden der Daten aus einer H5-Datei
def load_h5_file(file_path):
    all_data = []
    structure_info = []
    
    with h5py.File(file_path, 'r') as hf:
        # H5-Struktur erkunden
        structure_info = explore_h5_structure(hf)
        
        # Durchlaufe alle Gruppen in der H5-Datei (jede Gruppe entspricht einem Chat)
        for chat_id in hf.keys():
            chat_group = hf[chat_id]
            
            # Chat-Metadaten aus Attributen holen
            chat_name = chat_group.attrs.get('chat_name', f"Chat {chat_id}")
            unique_sender_count = chat_group.attrs.get('unique_sender_count', 0)
            message_count = chat_group.attrs.get('message_count', 0)
            
            # Prüfen, ob alle erforderlichen Datenfelder vorhanden sind
            required_fields = ['timestamp', 'sender_alias', 'message']
            if all(field in chat_group for field in required_fields):
                # Extrahiere Daten aus den Datasets
                try:
                    # Zeitstempel-Handling mit Fehlerbehandlung
                    timestamps = chat_group['timestamp'][:]
                    if 'timestamp_str' in chat_group:
                        # Verwende timestamp_str, falls vorhanden (für bessere Lesbarkeit)
                        timestamp_strings = [ts.decode('utf-8') if isinstance(ts, bytes) else str(ts) 
                                           for ts in chat_group['timestamp_str'][:]]
                        timestamps = [pd.to_datetime(ts) for ts in timestamp_strings]
                    else:
                        # Konvertiere Unix-Timestamps in datetime-Objekte
                        timestamps = [pd.to_datetime(ts, unit='s') if not np.isnan(ts) else pd.NaT 
                                     for ts in timestamps]
                    
                    # Sender und Nachrichten extrahieren
                    sender_aliases = [s.decode('utf-8') if isinstance(s, bytes) else str(s) 
                                     for s in chat_group['sender_alias'][:]]
                    messages = [m.decode('utf-8') if isinstance(m, bytes) else str(m) 
                               for m in chat_group['message'][:]]
                    
                    # Optional: message_ids extrahieren, falls vorhanden
                    message_ids = None
                    if 'message_id' in chat_group:
                        message_ids = chat_group['message_id'][:]
                    
                    # Optional: DeepL-Übersetzungen extrahieren, falls vorhanden
                    message_deepl = None
                    if 'message_deepl' in chat_group:
                        message_deepl = [m.decode('utf-8') if isinstance(m, bytes) else str(m) 
                                        for m in chat_group['message_deepl'][:]]
                    
                    # DataFrame erstellen
                    df_data = {
                        'timestamp': timestamps,
                        'sender_alias': sender_aliases,
                        'message': messages,
                    }
                    
                    if message_ids is not None:
                        df_data['message_id'] = message_ids
                    
                    if message_deepl is not None:
                        df_data['message_deepl'] = message_deepl
                    
                    df = pd.DataFrame(df_data)
                    
                    # Chat-Metadaten hinzufügen
                    df['chat_id'] = chat_id
                    df['chat_name'] = chat_name
                    df['file_name'] = os.path.basename(file_path)
                    
                    all_data.append(df)
                    
                except Exception as e:
                    st.error(f"Fehler beim Verarbeiten des Chats {chat_id}: {str(e)}")
    
    # Fasse alle DataFrames zusammen
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df, structure_info
    else:
        return pd.DataFrame(), structure_info

# Farben für Sender definieren
def get_sender_color(sender):
    colors = ["#FFDDC1", "#C1E1FF", "#D4FAC1", "#FFD1DC", "#E6E6FA"]  # Farbschema
    return colors[hash(sender) % len(colors)]  # Farbe je nach Sender

# Chat-Nachrichten formatieren mit Suchbegriff-Hervorhebung
def format_message(row, search_query=None, highlight_index=-1):
    try:
        timestamp = row["timestamp"].strftime("%d.%m.%Y %H:%M")
    except:
        timestamp = str(row["timestamp"])
    
    # Hellere Farbe für reguläre Nachrichten    
    color = get_sender_color(row["sender_alias"])
    
    # Suchbegriff gefunden? Dann hellere Hintergrundfarbe
    highlight_style = ""
    is_current_highlight = False
    
    if search_query:
        message_content = str(row['message']).lower()
        deepl_content = str(row.get('message_deepl', '')).lower()
        if search_query.lower() in message_content or search_query.lower() in deepl_content:
            # Helleren Hintergrund und einen Rahmen hinzufügen für alle Fundstellen
            highlight_style = "border: 2px solid #FF9933; background-color: #FFF7E6;"
            
            # Ist diese Nachricht die aktuell fokussierte Fundstelle?
            if row.name == highlight_index:
                # Stärkere Hervorhebung für die aktuelle Fundstelle
                highlight_style = "border: 3px solid #FF5500; background-color: #FFF0D9; box-shadow: 0 0 10px rgba(255, 153, 51, 0.5);"
                is_current_highlight = True
    
    # Anchor für automatisches Scrollen
    anchor = ""
    if is_current_highlight:
        anchor = f"<div id='current-highlight'></div>"
    
    message_text = f"{anchor}<div style='background-color:{color}; padding:8px; border-radius:5px; {highlight_style}'>"
    
    # Chatname über dem Sendernamen anzeigen, etwas kleiner, mit padding-bottom: 0
    message_text += f"<p style='font-size: 10px; color: #888; margin-bottom: 0px;'>{row['chat_name']}</p>"
    
    # Nachricht mit kleinerem Schriftstil
    message_text += f"<b style='font-size: 12px;'>{row['sender_alias']} ({timestamp}):</b><br>"
    
    # Entscheide, welchen Nachrichtentext anzeigen (DeepL oder Original)
    if 'message_deepl' in row and row['message_deepl'] and str(row['message_deepl']).strip():
        # Zeige nur die DeepL-Übersetzung an
        message_content = row['message_deepl']
        # Suchbegriff hervorheben, wenn vorhanden
        if search_query and search_query.lower() in message_content.lower():
            pattern = re.compile(re.escape(search_query), re.IGNORECASE)
            message_content = pattern.sub(f"<mark>{search_query}</mark>", message_content)
        message_text += f"<p style='font-size: 18px;'>{message_content}</p>"
    else:
        # Zeige die Originalnachricht an
        message_content = row['message']
        # Suchbegriff hervorheben, wenn vorhanden
        if search_query and search_query.lower() in message_content.lower():
            pattern = re.compile(re.escape(search_query), re.IGNORECASE)
            message_content = pattern.sub(f"<mark>{search_query}</mark>", message_content)
        message_text += f"<p style='font-size: 18px;'>{message_content}</p>"

    message_text += "</div>"
    return message_text

# Streamlit UI
st.title("📜 Chat Viewer")

# Eingabefeld für die H5-Datei mit Standardwert
default_file_path = "./chats.h5"
file_path = st.text_input("Gib den Pfad zur H5-Datei ein:", value=default_file_path, key="file_path_input")

if file_path:
    if os.path.exists(file_path) and file_path.endswith('.h5'):
        try:
            # Lade die H5-Datei und erhalte Strukturinformationen
            combined_df, structure_info = load_h5_file(file_path)
            
            # Anzeigen der H5-Struktur
            with st.expander("H5-Dateistruktur (zum Debugging)"):
                st.code("\n".join(structure_info))
            
            if not combined_df.empty:
                # Anzeigen einiger Statistiken
                st.write(f"### Statistiken")
                st.write(f"📊 Anzahl Chats: {combined_df['chat_id'].nunique()}")
                st.write(f"👥 Anzahl Sender: {combined_df['sender_alias'].nunique()}")
                st.write(f"💬 Anzahl Nachrichten: {len(combined_df)}")
                
                # Sortieren nach Zeitstempel
                combined_df.sort_values(["chat_id", "timestamp"], inplace=True)

                # Filter
                chat_ids = combined_df["chat_id"].unique()
                senders = combined_df["sender_alias"].unique()
                
                col1, col2 = st.columns(2)
                with col1:
                    selected_chat = st.selectbox("Wähle einen Chat", ["Alle"] + list(chat_ids), key="chat_selector")
                with col2:
                    selected_sender = st.selectbox("Wähle einen Sender", ["Alle"] + list(senders), key="sender_selector")
                
                search_query = st.text_input("🔍 Nachrichtensuche", key="search_query_input")

                # Zeitfilter
                min_date = combined_df["timestamp"].min().date()
                max_date = combined_df["timestamp"].max().date()

                # Sicherstellen, dass der Zeitraum gültig ist (min_date < max_date)
                if min_date == max_date:
                    # Falls min_date == max_date, setze max_date einen Tag weiter
                    max_date = min_date + timedelta(days=1)
                
                start_date, end_date = st.slider("📅 Zeitraum wählen", min_value=min_date, max_value=max_date, value=(min_date, max_date), key="date_range_slider")
                
                # Filter anwenden
                filtered_df = combined_df.copy()
                if selected_chat != "Alle":
                    filtered_df = filtered_df[filtered_df["chat_id"] == selected_chat]
                if selected_sender != "Alle":
                    filtered_df = filtered_df[filtered_df["sender_alias"] == selected_sender]
                
                # Zeitfilter anwenden
                filtered_df = filtered_df[(filtered_df["timestamp"].dt.date >= start_date) & (filtered_df["timestamp"].dt.date <= end_date)]
                
                # Suchfunktionalität: Suchbegriff speichern, aber nicht filtern
                search_query = st.text_input("🔍 Nachrichtensuche")
                search_results = []
                current_search_index = 0
                
                if search_query:
                    # Finde alle Indizes, wo die Suche übereinstimmt
                    search_filter = filtered_df["message"].str.contains(search_query, case=False, na=False)
                    if 'message_deepl' in filtered_df.columns:
                        search_filter |= filtered_df["message_deepl"].str.contains(search_query, case=False, na=False)
                    search_results = filtered_df[search_filter].index.tolist()
                    
                    if search_results:
                        # Navigation zwischen Suchergebnissen
                        col1, col2, col3, col4 = st.columns([3, 1, 1, 3])
                        
                        with col1:
                            st.success(f"{len(search_results)} Fundstellen für '{search_query}'")
                        
                        # Speichere aktuellen Suchindex in der Session
                        if 'search_index' not in st.session_state:
                            st.session_state.search_index = 0
                        
                        # Zurück-Button
                        with col2:
                            if st.button("◀ Vorherige", disabled=len(search_results) <= 1, key="prev_result_button"):
                                st.session_state.search_index = (st.session_state.search_index - 1) % len(search_results)
                                st.rerun()
                        
                        # Weiter-Button
                        with col3:
                            if st.button("Nächste ▶", disabled=len(search_results) <= 1):
                                st.session_state.search_index = (st.session_state.search_index + 1) % len(search_results)
                                st.rerun()
                        
                        # Aktuelle Position anzeigen
                        with col4:
                            if len(search_results) > 0:
                                st.info(f"Fundstelle {st.session_state.search_index + 1} von {len(search_results)}")
                        
                        current_search_index = st.session_state.search_index
                    else:
                        st.warning(f"Keine Ergebnisse für '{search_query}' gefunden")
                else:
                    # Zurücksetzen des Suchindex, wenn keine Suche aktiv ist
                    if 'search_index' in st.session_state:
                        del st.session_state.search_index
                # Nachrichten pro Seite
                msg_per_page = st.slider("Nachrichten pro Seite", min_value=10, max_value=100, value=25, step=5, key="msg_per_page_slider")
                
                # Paginierung - bei Suche zur ersten Fundstelle springen
                total_msgs = len(filtered_df)
                total_pages = (total_msgs - 1) // msg_per_page + 1 if total_msgs > 0 else 1
                
                # Bestimme Startseite basierend auf Suchergebnissen
                default_page = 1
                if search_results:
                    # Verwende den aktuellen Suchindex, um zur richtigen Seite zu springen
                    result_index = filtered_df.index.get_loc(search_results[current_search_index])
                    default_page = (result_index // msg_per_page) + 1
                
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    current_page = st.number_input("Seite", min_value=1, max_value=total_pages, value=default_page, step=1, key="page_number_input")
                
                # Berechne Indizes für die aktuelle Seite
                start_idx = (current_page - 1) * msg_per_page
                end_idx = min(start_idx + msg_per_page, total_msgs)
                
                # Anzeigeoptionen für Übersetzungen
                display_option = "DeepL Übersetzung bevorzugt"
                if 'message_deepl' in filtered_df.columns:
                    display_option = st.radio(
                        "Anzeigeoptionen:",
                        ["DeepL Übersetzung bevorzugt", "Nur Originalnachrichten", "Beide anzeigen (Original & Übersetzung)"],
                        horizontal=True,
                        key="display_option_radio"
                    )
                
                # Nach dem Rendern der Nachrichten, füge JavaScript für Auto-Scrolling ein
                if search_results and 'search_index' in st.session_state:
                    # JavaScript um automatisch zur aktuellen Fundstelle zu scrollen
                    st.markdown("""
                    <script>
                        // Funktion zum Scrollen zur aktuellen Fundstelle
                        function scrollToCurrentHighlight() {
                            const highlight = document.getElementById('current-highlight');
                            if (highlight) {
                                highlight.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            }
                        }
                        
                        // Nach dem Laden der Seite aufrufen
                        window.addEventListener('load', function() {
                            setTimeout(scrollToCurrentHighlight, 500);
                        });
                    </script>
                    """, unsafe_allow_html=True)
                
                if total_msgs > 0:
                    # Je nach Auswahl den Nachrichtentext anpassen
                    current_highlight_index = -1
                    if search_results and current_search_index < len(search_results):
                        current_highlight_index = search_results[current_search_index]
                    
                    page_df = filtered_df.iloc[start_idx:end_idx]
                    for _, row in page_df.iterrows():
                        # Je nach Auswahl den Nachrichtentext anpassen
                        if display_option == "Nur Originalnachrichten":
                            row_copy = row.copy()
                            if 'message_deepl' in row_copy:
                                row_copy['message_deepl'] = ""
                            st.markdown(format_message(row_copy, search_query, current_highlight_index), unsafe_allow_html=True)
                        elif display_option == "Beide anzeigen (Original & Übersetzung)":
                            row_copy = row.copy()
                            if 'message_deepl' in row_copy and row_copy['message_deepl'] and row_copy['message_deepl'] != row_copy['message']:
                                message_with_translation = f"{row_copy['message']}<br><i style='color: #666;'>Übersetzung: {row_copy['message_deepl']}</i>"
                                row_copy['message'] = message_with_translation
                                row_copy['message_deepl'] = ""
                            st.markdown(format_message(row_copy, search_query, current_highlight_index), unsafe_allow_html=True)
                        else:
                            # DeepL bevorzugt (Standard)
                            st.markdown(format_message(row, search_query, current_highlight_index), unsafe_allow_html=True)
                        st.markdown("---")
                else:
                    st.info("Keine Nachrichten gefunden, die den Filterkriterien entsprechen.")
            else:
                st.warning("Keine Chat-Daten in der H5-Datei gefunden. Bitte überprüfe die Dateistruktur im ausgeklappten Bereich oben.")
        except Exception as e:
            st.error(f"Fehler beim Laden der H5-Datei: {str(e)}")
    else:
        st.error("Die angegebene Datei existiert nicht oder ist keine H5-Datei.")
else:
    # Wenn keine Datei angegeben wurde
    st.write("Bitte gib den Pfad zu einer H5-Datei ein, die die Chat-Daten enthält.")
    
    # Anleitung zum Konvertieren von JSON nach H5
    with st.expander("JSON zu H5 konvertieren"):
        st.markdown("""
        ### Anleitung zum Konvertieren von JSON zu H5
        
        Wenn du eine JSON-Datei im folgenden Format hast:
        ```json
        [
          {
            "chat_id": "!BBdqOaakPevDUaGcsA:matrix.example.com",
            "unique_sender_count": 1,
            "message_count": 2,
            "messages": [
              {
                "timestamp": "2023-09-19 13:12:58",
                "sender_alias": "@username:matrix.example.com",
                "message": "Beispielnachricht",
                "message_id": 199
              },
              ...
            ]
          },
          ...
        ]
        ```
        
        Kannst du das beigefügte Konvertierungsscript verwenden:
        ```
        python json_to_h5_converter.py meine_daten.json
        ```
        
        Dies erzeugt eine H5-Datei mit dem gleichen Namen (meine_daten.h5), die mit diesem Chat Viewer kompatibel ist.
        """)