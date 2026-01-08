import os
from bs4 import BeautifulSoup
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGO_URI = "mongodb+srv://admin:test1234@cluster0.umjyj.mongodb.net/quran_db?retryWrites=true&w=majority&appName=Cluster0"

DB_NAME = 'quran_db'
COLLECTION_NAME = 'verses'

# Dateinamen (müssen mit deinen Dateien im Ordner übereinstimmen)
FILES = {
    'arabic': 'quran.xml',
    'abu_rida': 'de.quran.abu_rida.xml',
    'bubenheim': 'de.bubenheim.xml',
    'khoury': 'de.khoury.xml',
    'zaidan': 'de.zaidan.xml'
}


def run_etl():
    print("🚀 Starte ETL-Prozess für Quranverse...")

    # 2. ALLE DATEIEN PARSEN
    soups = {}
    try:
        for key, filename in FILES.items():
            print(f"Lese Datei: {filename}...")
            with open(filename, 'r', encoding='utf-8') as f:
                soups[key] = BeautifulSoup(f, 'xml')
        print("✅ Alle XML-Dateien erfolgreich eingelesen.")
    except FileNotFoundError as e:
        print(f"❌ FEHLER: Datei nicht gefunden! {e}")
        return

    # 3. DATEN ZUSAMMENFÜHREN
    print("⏳ Verarbeite Daten und verknüpfe Übersetzungen...")
    quran_data = []

    # Wir iterieren durch die arabische Struktur als Basis
    arabic_suras = soups['arabic'].find_all('sura')

    for ar_sura in arabic_suras:
        index = ar_sura['index']
        name_ar = ar_sura['name']

        # Wir suchen die entsprechende Sure in den deutschen Dateien
        sura_abu = soups['abu_rida'].find('sura', index=index)
        sura_bub = soups['bubenheim'].find('sura', index=index)
        sura_kho = soups['khoury'].find('sura', index=index)
        sura_zai = soups['zaidan'].find('sura', index=index)

        # Alle Verse der aktuellen Sure durchgehen
        ar_ayas = ar_sura.find_all('aya')

        for ar_aya in ar_ayas:
            aya_idx = ar_aya['index']

            # Das Herzstück: Wir bauen das komplexe Objekt
            verse_obj = {
                "sura_index": int(index),
                "aya_index": int(aya_idx),
                "sura_name_ar": name_ar,
                "arabic_text": ar_aya['text'],
                # Hier ist die neue Struktur für dein Vue Frontend:
                "translations": {
                    "abu_rida": sura_abu.find('aya', index=aya_idx)['text'],
                    "bubenheim": sura_bub.find('aya', index=aya_idx)['text'],
                    "khoury": sura_kho.find('aya', index=aya_idx)['text'],
                    "zaidan": sura_zai.find('aya', index=aya_idx)['text']
                }
            }
            quran_data.append(verse_obj)

    print(f"✅ Datenstruktur erstellt: {len(quran_data)} Verse verarbeitet.")

    # 4. IN MONGODB LADEN
    try:
        print("uud4 Verbinde mit MongoDB Atlas...")
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Alte Daten löschen (wichtig, damit wir keine Duplikate oder alte Strukturen haben)
        print("🗑️ Lösche alte Daten...")
        collection.delete_many({})

        # Neue Daten einfügen
        print("uud4 Lade neue Daten hoch...")
        collection.insert_many(quran_data)

        # Text-Index erneuern (damit die Suche auch die neuen Felder findet)
        # Wir indexieren alle Übersetzungen für die Volltextsuche
        print("uud4 Aktualisiere Such-Index...")
        collection.drop_indexes()
        collection.create_index([
            ("german_text", "text"),  # Fallback
            ("translations.abu_rida", "text"),
            ("translations.bubenheim", "text"),
            ("translations.khoury", "text"),
            ("translations.zaidan", "text"),
            ("arabic_text", "text")
        ])

        print(f"🎉 FERTIG! {len(quran_data)} Verse sind jetzt in der 'Pro'-Version online.")

    except Exception as e:
        print(f"❌ Datenbank-Fehler: {e}")


if __name__ == "__main__":
    run_etl()