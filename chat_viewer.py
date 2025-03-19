import os
import json
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

# Funktion zum Laden der JSON-Daten aus mehreren Dateien
def load_multiple_json_files(directory):
    all_data = []
    
    # Durchlaufe alle Dateien und Ordner im angegebenen Verzeichnis
    for foldername, subfolders, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith(".json"):
                file_path = os.path.join(foldername, filename)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    # Füge Chat-ID und andere Metadaten hinzu, falls nötig
                    df = pd.DataFrame(data["messages"]).assign(chat_id=data["chat_id"], chat_name=data.get("chat_name", f"Chat {data['chat_id']}"))
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                    df["file_name"] = filename  # Dateiname hinzufügen
                    all_data.append(df)
    
    # Fasse alle DataFrames zusammen
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df

# Farben für Sender definieren
def get_sender_color(sender):
    colors = ["#FFDDC1", "#C1E1FF", "#D4FAC1", "#FFD1DC", "#E6E6FA"]  # Farbschema
    return colors[hash(sender) % len(colors)]  # Farbe je nach Sender

# Chat-Nachrichten formatieren
def format_message(row):
    timestamp = row["timestamp"].strftime("%d.%m.%Y %H:%M")
    color = get_sender_color(row["sender_alias"])
    message_text = f"<div style='background-color:{color}; padding:8px; border-radius:5px;'>"
    
    # Chatname über dem Sendernamen anzeigen, etwas kleiner
    message_text += f"<p style='font-size: 10px; color: #888;'>{row['chat_name']}</p>"  # Chatname kleiner
    
    # Nachricht mit kleinerem Schriftstil
    message_text += f"<b style='font-size: 12px;'>{row['sender_alias']} ({timestamp}):</b><br>"  # Sendername etwas kleiner
    
    if 'message_deepl' in row:
        message = f"{row['message_deepl']}"
    else:
        message = f"{row['message']}"
        
    message_text += f"<p style='font-size: 12px;'>{message}</p>"  # Nachricht selbst etwas kleiner
    
    message_text += "</div>"
    return message_text

# Streamlit UI
st.title("📜 Chat Viewer")

# Eingabefeld für den Ordnerpfad
folder_path = st.text_input("Gib den Pfad zum Ordner mit den JSON-Dateien ein:")

if folder_path:
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        # Lade alle JSON-Dateien im angegebenen Ordner
        combined_df = load_multiple_json_files(folder_path)
        combined_df.sort_values(["chat_id", "timestamp"], inplace=True)

        # Filter
        chat_ids = combined_df["chat_id"].unique()
        senders = combined_df["sender_alias"].unique()
        selected_chat = st.selectbox("Wähle einen Chat", ["Alle"] + list(chat_ids))
        selected_sender = st.selectbox("Wähle einen Sender", ["Alle"] + list(senders))
        search_query = st.text_input("🔍 Nachrichtensuche")

        # Zeitfilter
        min_date = combined_df["timestamp"].min().date()
        max_date = combined_df["timestamp"].max().date()

        # Sicherstellen, dass der Zeitraum gültig ist (min_date < max_date)
        if min_date == max_date:
            # Falls min_date == max_date, setze max_date einen Tag weiter
            max_date = min_date + timedelta(days=1)
        
        start_date, end_date = st.slider("📅 Zeitraum wählen", min_value=min_date, max_value=max_date, value=(min_date, max_date))
        
        # Filter anwenden
        filtered_df = combined_df.copy()
        if selected_chat != "Alle":
            filtered_df = filtered_df[filtered_df["chat_id"] == selected_chat]
        if selected_sender != "Alle":
            filtered_df = filtered_df[filtered_df["sender_alias"] == selected_sender]
        if search_query:
            filtered_df = filtered_df[filtered_df["message"].str.contains(search_query, case=False, na=False) |
                                      filtered_df["message_deepl"].str.contains(search_query, case=False, na=False)]
        filtered_df = filtered_df[(filtered_df["timestamp"].dt.date >= start_date) & (filtered_df["timestamp"].dt.date <= end_date)]
        
        st.write("### Gefilterter Chatverlauf")
        for _, row in filtered_df.iterrows():
            st.markdown(format_message(row), unsafe_allow_html=True)
            st.markdown("---")
    else:
        st.error("Der angegebene Ordnerpfad existiert nicht oder ist kein Verzeichnis.")
else:
    # Wenn kein Ordnerpfad angegeben wurde
    st.write("Bitte gib den Pfad zu einem Ordner ein, der die JSON-Dateien enthält.")
