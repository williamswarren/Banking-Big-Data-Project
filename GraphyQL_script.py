from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import csv
import os

api_key = 'YOUR YELP API KEY'

# define our authentication process.
header = {'Authorization': 'bearer {}'.format(api_key),
          'Content-Type':"application/json"}

# Build the request framework
transport = RequestsHTTPTransport(url='https://api.yelp.com/v3/graphql', headers=header, use_json=True)

# Create the client
client = Client(transport=transport, fetch_schema_from_transport=True)

# Search terms for banking/credit card
search_term_list = ['credit card', 'ApplePay', 'GooglePay', 'bank', 'money', 'credit score', 'pre-approved', 'cashback', 'credit points', 'interest rates']

# Cities data set from government source, all cities across the United States
def make_cities(file):
    cities_list = []
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        count = -1
        for row in csv_reader:
            count += 1
            if count == 0:
                continue
            cities_list.append(row[0])
    return cities_list

search_cities = make_cities('/Users/warrenwilliams/Downloads/simplemaps_uscities_basicv1.73/uscities.csv')

query8 = gql("""{{
  search(term:"{}",
         location:"{}",
         sort_by: "rating") {{
    total
    business {{
      name
      reviews {{
        text
        rating
      }}
    }}
  }}
}}""".format('credit card', 'United States'))

#query8 = gql(query8)
# Get GraphQL response from Yelp & append responses to txt file

def query_append_to_file(text_file, term, city):
    API_CALLS = 0
    with open(text_file,'a') as writer:

        search_term_list_count = term -1
        search_city_list_count = city -1

        for i in range(term, len(search_term_list)):
            search_term_list_count += 1
            for j in range(city, len(search_cities)):
                search_city_list_count += 1
                try:
                    query7 = gql("""{{
                    search(term:"{}",
                            location:"{}",
                            sort_by: "rating") {{
                        total
                        business {{
                        name
                        reviews {{
                            text
                            rating
                          }}
                        }}
                      }}
                    }}""".format(search_term_list[i],search_cities[j]))

                    result = client.execute(query7)
                    API_CALLS += 1
                    print("Number of API Calls is: " + str(API_CALLS))
                    for business in result['search']['business']:
                        for customers in business['reviews']:
                            writer.write(f'''\n{customers['text']}''')

                except:
                    print(result)
                    print(search_term_list_count, search_city_list_count)
                    return search_term_list_count, search_city_list_count
        return search_term_list_count, search_city_list_count

#print('-'*100)
#result = client.execute(query7)
#print(result['search']['business'][0]['reviews'][0]['text'])

text_file = '/Users/warrenwilliams/Documents/School/UCSC Extension/Quarter 2/Big Data Overview And Use Cases/graphqlyelp/bankreviews.txt'

filename = './program_count.txt'
if os.path.exists(filename) == False:
    print(filename + " does not exist")
    exit(1)

fileobj = open(filename, 'r')
contents = fileobj.read()
contents = contents.strip().strip('\n\r')
contents = contents.split(',')
fileobj.close()

term = int(contents[0])
city = int(contents[1])

results = query_append_to_file(text_file, term, city)

filename = 'program_count.txt'
os.system('rm -f %s ' % filename)
bashcmd = 'echo ' + f'{str(results[0])},{str(results[1])}' + ' > ' + filename
os.system(bashcmd)

query6 = gql('''{
  search(term:"credit card",
         location:"United States",
         sort_by: "rating") {
    total
    business {
      name
      reviews {
        text
        rating
      }
    }
  }
}
''')