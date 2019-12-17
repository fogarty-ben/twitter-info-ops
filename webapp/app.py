import csv
import io
import json
from threading import Thread

from flask import Flask, render_template, request, jsonify, redirect
import happybase
from kafka import KafkaProducer

HOST = '0.0.0.0'
PORT = 3043
app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667')

@app.route('/tweets.html')
def index():
    return(render_template('tweets.html'))

@app.route('/results.html')
def results():
    phrase = request.args.get('term', None);
    if not phrase:
        return 'No terms entered!'
    raw_phrase = phrase
    connection = happybase.Connection(host='mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal',
                                 port=9090)
    
    # select whether to user hashtags or ngrams tables
    if phrase[0] == '#':
        phrase = phrase[1:] 
        by_month_table = connection.table('benfogarty_hashtags_by_month')
        speed_table = connection.table('benfogarty_hashtags_speed')
        top_users_table = connection.table('benfogarty_hashtags_top_users')
        prefix = 'hashtags:'
        encoded_prefix = prefix.encode('utf-8')
    else:
        by_month_table = connection.table('benfogarty_ngrams_by_month')
        speed_table = connection.table('benfogarty_ngrams_speed')
        top_users_table = connection.table('benfogarty_ngrams_top_users')
        prefix = 'ngrams:'
        encoded_prefix = prefix.encode('utf-8')
    
    phrase = phrase.lower()
    encoded_phrase = phrase.encode('utf-8')
 
    speed_updates = {}
    for key, data in speed_table.scan(row_prefix=encoded_phrase + b'_', columns=[]):
        term, month = key.decode('utf-8').split('_')
        speed_updates[key] = {'count': speed_table.counter_get(key, encoded_prefix + b'count#b'),
                                'likes': speed_table.counter_get(key, encoded_prefix + b'total_like_count#b'),
                                'quotes': speed_table.counter_get(key, encoded_prefix + b'total_quote_count#b'),
                                'replies': speed_table.counter_get(key, encoded_prefix + b'total_reply_count#b'),
                                'retweets': speed_table.counter_get(key, encoded_prefix + b'total_retweet_count#b')}
                     
    # retrive by month data for chart
    chart_data = []
    for key, data in by_month_table.scan(row_prefix=encoded_phrase + b'_'):
        data_dict = {'month': data[encoded_prefix + b'tweet_month'].decode('utf-8'),
                'count': int(data[encoded_prefix + b'count'].decode('utf-8')),
                'likes': int(data[encoded_prefix + b'total_like_count'].decode('utf-8')),
                'quotes': int(data[encoded_prefix + b'total_quote_count'].decode('utf-8')),
                'replies': int(data[encoded_prefix + b'total_reply_count'].decode('utf-8')),
                'retweets': int(data[encoded_prefix + b'total_retweet_count'].decode('utf-8'))}
        
        if key in speed_updates:
            data_dict['count'] += speed_updates[key].get('count', 0)
            data_dict['likes'] += speed_updates[key].get('likes', 0)
            data_dict['quotes'] += speed_updates[key].get('quotes', 0)
            data_dict['replies'] += speed_updates[key].get('replies', 0)
            data_dict['retweets'] += speed_updates[key].get('retweets', 0)

        chart_data.append(data_dict)
    
    table_data = top_users_table.row(encoded_phrase)
    table_data = {key.decode('utf-8'): val.decode('utf-8').split('\t,\t')
                  for key, val in table_data.items()}

    # retrive top user data
    formatted_table_data = []
    for i in range(len(table_data.get(prefix + 'user_display_names', []))):
            display_name = table_data[prefix + 'user_display_names'][i]
            handle = table_data[prefix + 'user_screen_names'][i]
            if len(display_name) == 64 and len(handle) == 64:
                display_name = '<Redacted>'
                handle = '<Redacted>'
            bio = table_data[prefix + 'user_profile_descriptions'][i]
            location = table_data[prefix + 'user_reported_locations'][i]
            engagement = int(table_data[prefix + 'total_engagement_counts'][i])
            retweets = int(table_data[prefix + 'total_retweet_counts'][i])
            likes = int(table_data[prefix + 'total_like_counts'][i])
            quotes = int(table_data[prefix + 'total_quote_counts'][i])
            replies = int(table_data[prefix + 'total_reply_counts'][i])
            user = {'display_name': display_name,
                    'handle': handle,
                    'bio': bio,
                    'location': location,
                    'engagement': engagement,
                    'retweets': retweets,
                    'likes': likes,
                    'quotes': quotes,
                    'replies': replies}
            formatted_table_data.append(user)
    
    return render_template('results.html', phrase=raw_phrase, chart_data=chart_data,
                           table_data=formatted_table_data) 

@app.route('/submit-tweets.html')
def submit_tweets():
    return render_template('submit-tweets.html')

def send_messages(f): 
    if not f:
        pass
    
    # format the file for DictReader
    f = f.read().decode('utf-8')
    f = io.StringIO(f)
    
    fileReader = csv.DictReader(f)
    for row in fileReader:
        kafkaMessage = json.dumps(row).encode('utf-8')
        producer.send('benfogarty_tweets', kafkaMessage)
    
@app.route('/add-tweets.html', methods=['POST'])
def add_tweets():
    f = request.files['newTweets']
    send_messages(f)
    return redirect('submit-tweets.html')

if __name__ == '__main__':
    app.run(host=HOST, port=PORT)
