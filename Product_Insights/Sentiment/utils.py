from google.cloud import language, translate
from google.cloud.language import enums, types


language_client = language.LanguageServiceClient()
translate_client = translate.Client()

def gc_detect_language(text):
    result = translate_client.detect_language(text)
    return(result['confidence'], result['language'])
    
def gc_sentiment(text, type=enums.Document.Type.PLAIN_TEXT,
                 language='en'):  

    document = types.Document(
            content=text,
            type=type,
            language=language)
    annotations = language_client.analyze_sentiment(document=document)
    score = annotations.document_sentiment.score
    magnitude = annotations.document_sentiment.magnitude
    return(score, magnitude)

def discretize_sentiment(score, magnitude, score_cutoff=0.2, magnitude_cutoff=0.5):
    if magnitude < magnitude_cutoff:
        return(u'neutral') 
    elif score < -score_cutoff:
        return(u'negativ')
    elif -score_cutoff <= score <= score_cutoff:
        return(u'neutral')
    elif score > score_cutoff:
        return(u'positive') 

