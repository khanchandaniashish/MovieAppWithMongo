from pymongo import MongoClient, ReadPreference
from sshtunnel import SSHTunnelForwarder
from pprint import pprint

#MONGO_HOST = '18.208.164.105'
MONGO_HOST = '44.201.127.187'
USER = 'ubuntu'
PRIVATE_KEY = '/Users/ashish/Downloads/AWSKeyTeam.pem'

server = SSHTunnelForwarder(MONGO_HOST, ssh_username = USER, ssh_pkey=PRIVATE_KEY, remote_bind_address = ('127.0.0.1', 27017))
server.start()
print("Successfully established connection to:" + MONGO_HOST + "\n")

client = MongoClient('localhost', server.local_bind_port, read_preference = ReadPreference.PRIMARY_PREFERRED)
# client = MongoClient('mongodb://ec2-18-208-164-105.compute-1.amazonaws.com:27017', ssl=True, ssl_ca_certs = "./AWSKeyTeam.pem")
db = client.MovieLens
collection = db.movies

#Find all movies
def find_all_movies() :
    #note: added limit/$slice temporarily
    for movie in collection.find({}, {"_id": 1, "item_id": 1, "title": 1, "directedBy": 1, "starring": 1,
                                       "avgRating": 1, "imdbId": 1, "txt": {"$slice": 1}}).limit(3):
        print(movie)

# Filter by criteria
def find_movies_by_criteria(criteria) :
    
    for movie in collection.find({"item_id":int(criteria)}, {"_id": 1, "item_id": 1, "title": 1, "directedBy": 1, "starring": 1,
                                       "avgRating": 1, "imdbId": 1, "txt": {"$slice": 1}}).limit(1):
        print(movie)
    

# for result in results:
#     print(result)
    
#     # note: added $slice temporarily
#     for movie in collection.find(str(criteria), {"_id": 1, "item_id": 1, "title": 1, "directedBy": 1, "starring": 1,
#                                        "avgRating": 1, "imdbId": 1, "txt": {"$slice": 1}}):
#         print(movie)

# Movies with the lowest rating
def find_worst_movies(n):
    for movie in collection.find({"title": 1, "directedBy": 1, "starring": 1, "avgRating": 1}).sort({"avgRating": 1}).limit(n):
        print(movie)

#  Add bookmark field
def update_bookmark(title):
    filter = {"title": {"$regex": "/"+title+"/"}}
    update = {"$set": {"bookmarked": "true"}}
    result = collection.update_many(filter, update, upsert=False)
    print(result)

    for movie in collection.find({"bookmarked": "true"}, {"title": 1, "avgRating": 1, "bookmarked": 1}):
        print(movie)


#  Add bookmark field
def update_title(item_id,newtitle):

    myquery = { "item_id": int(item_id) }
    newvalues = { "$set": { "title": str(newtitle) } }

    collection.update_one(myquery, newvalues)

    #print "movies" after the update:
    # for x in collection.find().limit(1):
    #     print(x)
   


# Add a new movie
def add_movie(movie):
    result = collection.insert_one(movie)
    print(result)
    movie = collection.find_one({"item_id": movie['item_id']})
    print(movie)

# Delete an existing movie by item_id
def delete_movie(item_id):
    result = collection.delete_one({"item_id": item_id})
    print(result)
    movie = collection.find_one({"item_id": item_id})
    print(movie)

def user_input():
    cont = True
    while cont:
        func = int(input("Which function would you like to call? \n1. Find all movies \n2. Find movie by item_id \n3. Find movies by additional criteria "
              "\n4. Find movies by cast \n5. Find worst movies \n6. Update title for a movie \n7. Add movie \n8. Delete movie by item_id \n"))
        if func == 1: find_all_movies()
        elif func == 2:
            title = input("\nItem_id of the movie to search: ")
            find_movies_by_criteria(title)
        elif func == 3:
            criteria = {}
            directedBy = input("\nDirector? (n to skip)")
            if directedBy != "n": criteria['directedBy'] = directedBy
            avgRating = input("\nAverage rating? (n to skip)")
            if avgRating != "n": criteria['avgRating'] = avgRating
            cast = input("\nCast? (n to skip)")
            if cast != "n": criteria['starring'] = {"$regex": "/"+cast+"/"}
            imdbId = input("\niMDb id? (n to skip)")
            if imdbId != "n": criteria['imdbId'] = imdbId
            find_movies_by_criteria(criteria)
        elif func == 4:
            cast = input("\nCast: ")
            find_movies_by_criteria({"starring": {"$regex": "/"+cast+"/"}})
        elif func == 5:
            find_worst_movies(input("\nWorst movies count: "))
        elif func == 6:
            item_id = input("\nWhich Movie item_id to update? " )
            newtitle = input("\nNew Title ? ")
            update_title(item_id,newtitle)
            print("Update complete!")
            # update_bookmark(title)
        elif func == 7:
            title = input("\nTitle: ")
            directedBy = input("\nDirector: ")
            cast = input("\nCast: ")
            avgRating = int(input("\nAverage rating: "))
            item_id = int(input("\nItem_id: "))
            imdbId = int(input("\niMDb id: "))
            add_movie({"title": title, "directedBy": directedBy, "starring": cast, "avgRating": avgRating, "item_id": item_id, "imdbId": imdbId})
        elif func == 8:
            item_id = int(input("\nItem_id of movie to delete: "))
            delete_movie(item_id)
        else: print("\nPlease input a valid operation number: ")

        cont = (input("Continue? (y/n)") == 'y')

print("Connected to Mongo movie database.\n")
user_input()
