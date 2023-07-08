import nltk
from nltk.tokenize import word_tokenize, PunktSentenceTokenizer
from nltk.corpus import stopwords
from nltk.tag import pos_tag

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

def extract_nouns(tweet):
    if type(tweet)==type("tweet"):
        sentence_tokenizer = PunktSentenceTokenizer()
        sentences = sentence_tokenizer.tokenize(tweet)
        words = [word_tokenize(sentence) for sentence in sentences]

        tagged_words = [pos_tag(sentence) for sentence in words]
        nouns = []
        for sentence in tagged_words:
            nouns += [word for word, pos in sentence if pos.startswith('N')]

        stopwords_list = set(stopwords.words('english'))
        nouns = [noun for noun in nouns if noun.lower() not in stopwords_list]
        return str(nouns)
    else:
        return "[]"

    
    

    