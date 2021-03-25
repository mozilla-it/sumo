from google.cloud import translate_v2 as translate
from google.cloud import language
from google.cloud.language_v1 import enums, types


language_client = language.LanguageServiceClient()
translate_client = translate.Client()

def gc_detect_language(text):
    """Calls the Google Cloud Language detection API"""

    if len(text.encode("utf-8")) > 2000:
        #return("0", "EN") # shrug
        text = text[:2000]
    result = translate_client.detect_language(text)
    return(result['confidence'], result['language'])
    
def gc_sentiment(text, type=enums.Document.Type.PLAIN_TEXT,
                 language='en'):  
    """Calls the Google Cloud Sentiment Analysis API"""

    if len(text.encode("utf-8")) > 5000:
        text = text[:5000]
    document = types.Document(
            content=text,
            type=type,
            language=language)
    annotations = language_client.analyze_sentiment(document=document)
    score = annotations.document_sentiment.score
    magnitude = annotations.document_sentiment.magnitude
    return(score, magnitude)

def discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5):
    """Transforms the sentiment score and magnitude into positive, neutral or negative.

    The Google Cloud Sentiment Analysis API returns both a sentiment score and a
    sentiment magnitude. The score indicates the overall emotion of a document.
    The magnitude indicates how much emotional content is present. This function 
    transforms this first by looking at the magnitude. If the magnitude is less 
    than the magnitude_cutoff, then the statement is interpreted as being neutral. 
    If the magnitude is greater, but the absolute value of the score is less
    than the score_cutoff, then the statement is also interpreted as being neutral. 
    If the score is greater than the score_cutoff then statement is positive. 
    If the score is less than the minus value of score_cutoff then the statement is negative.
    """

    if magnitude < magnitude_cutoff:
        return(u'neutral') 
    elif score < -score_cutoff:
        return(u'negativ')
    elif -score_cutoff <= score <= score_cutoff:
        return(u'neutral')
    elif score > score_cutoff:
        return(u'positive') 

