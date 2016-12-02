"""
@author: Weijian
"""
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz


def get_scikit_fuzz(name1, name2):
    documents=(name1,name2)

    tfidf_vectorizer = TfidfVectorizer(analyzer="char")
    tfidf_matrix = tfidf_vectorizer.fit_transform(documents)
    cs = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix)
    return cs


def get_fuzz(name1, name2):
    return fuzz.partial_ratio( name1, name2)
