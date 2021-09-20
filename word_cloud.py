import requests
import json
from collections import defaultdict
from wordcloud import WordCloud
import matplotlib.pyplot as plot
from nltk.corpus import stopwords

KEY = '610d01c5242f4593a6d11d5cca266ae5'
URL = ('https://newsapi.org/v2/everything?'
       'q=russia&'
       'pageSize=100&'
       'from=2021-09-01&'
       f'apiKey={KEY}')
STOP_WORDS = set(stopwords.words('english'))


def get_data(url):
    response = requests.get(url)
    if response.ok:
        result_json = json.loads(response.text)
        print(result_json)
        return result_json
    else:
        response.raise_for_status()

def get_word_cloud(data):
    keywords = []
    for news in data['articles']:
           keywords.extend(news['title'].split(' '))
    keywords = [word for word in keywords if not word in STOP_WORDS and word != '-']
    top_keywords = defaultdict(int)
    for word in keywords:
           top_keywords[word] += 1
    top_keywords_sorted = dict(sorted(top_keywords.items(), key=lambda x: x[1], reverse=True)[:50])
    return top_keywords_sorted


def draw_world_cloud(word_cloud):
    cloud_to_draw = WordCloud(width=450,
                              height=250,
                              max_words=50,
                              background_color="black",
                              relative_scaling=1,
                              normalize_plurals=False,
                              collocations=False)\
                              .generate_from_frequencies(word_cloud)

    plot.imshow(cloud_to_draw, interpolation='bilinear')
    plot.axis('off')
    plot.show()


if __name__ == '__main__':
    data = get_data(URL)
    word_cloud = get_word_cloud(data)
    draw_world_cloud(word_cloud)