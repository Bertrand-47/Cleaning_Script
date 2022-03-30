from config import config
import nltk
import psycopg2
import pandas as pd
import re
import unicodedata


def fetch_product_info():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()

        # retrieve all ebay products in db
        query = 'SELECT model_data.item_info.item_id, model_data.item_info.orig_title, model_data.item_info.orig_description FROM model_data.item_info'
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['item_id', 'title', 'description'])

        # close the communication with the PostgreSQL db
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return df


def remove_stopwords(texts, stop_words):
    """List of strings is expected as an input"""
    out = []
    for doc in texts:
        doc_words = ""
        for word in nltk.word_tokenize(str(doc)):
            if word not in stop_words:
                doc_words += word
                doc_words += " "
        # remove trailing whitespace before appending
        doc_words = doc_words[0:-1]
        out.append(doc_words)
    return out


def clean_description(texts, stop_words):
    """"Remove  symbols, HTML tags, and identified unnecessary information (e.g., product quality descriptions)"""
    out = []
    for doc in texts:
        # remove all text between a series of four hashtags, non-greedy (removes product quality descriptions)
        new_doc = re.sub(r"####.+?####", '', doc)
        # remove HTML tags by removing all text between angle brackets, non-greedy
        new_doc = re.sub(r"<.+?>", "", new_doc)
        # replace any occurrence of hyphens with an underscore
        new_doc = re.sub(r"-+", "_", new_doc)
        # remove all remaining non-alphanumeric characters, preserving underscores and whitespace
        new_doc = re.sub(r"[^a-zA-Z0-9_\s]", "", new_doc)

        out.append(new_doc)

    # remove stopwords from the remaining texts
    out = remove_stopwords(out, stop_words)
    return out

def clean_string(string):
    """Performs all necessary cleaning on the string. (Converts to lowercase, removes unneccesary symbols and stop words)"""
    # Changes characters with accents to normal form
    result = ''.join((c for c in unicodedata.normalize('NFD', string) if unicodedata.category(c) != 'Mn'))
    result = result.lower()
    stop_words = nltk.corpus.stopwords.words('english')
    # Removes symbols and unnecessary information
    out = clean_description([result], stop_words)
    return out[0]

if __name__ == "__main__":
    # fill dataframe with relevant info
    df = fetch_product_info().astype(str)

    # NOTE: Uncomment the download commands if running for the first time
    # nltk.download('stopwords')
    # nltk.download('punkt')
    stop_words = nltk.corpus.stopwords.words('english')

    df["modified_title"] = remove_stopwords(df["title"], stop_words)
    df["modified_description"] = clean_description(df["description"], stop_words)

    df.to_csv("preprocessed_product_info.csv", encoding='utf-8', index=False)