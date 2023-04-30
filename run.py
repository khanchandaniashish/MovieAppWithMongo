from pymongo import MongoClient, ReadPreference
from sshtunnel import SSHTunnelForwarder
from  pymongo import ASCENDING,DESCENDING

#MONGO_HOST = '18.208.164.105'
MONGO_HOST = '44.204.49.238'
USER = 'ubuntu'
PRIVATE_KEY = '/Users/ashish/Downloads/AWSKeyTeam.pem'

server = SSHTunnelForwarder(MONGO_HOST, ssh_username = USER, ssh_pkey=PRIVATE_KEY, remote_bind_address = ('127.0.0.1', 27017))
server.start()
print("Successfully established connection to:" + MONGO_HOST + "\n")

client = MongoClient('localhost', server.local_bind_port, read_preference = ReadPreference.PRIMARY_PREFERRED)
db = client.MovieLens
collection = db.movies

#Find all movies
def find_all_movies() :
    #note: added limit/$slice temporarily
    for movie in collection.find({}, {"_id": 1, "item_id": 1, "title": 1, "directedBy": 1, "starring": 1,
                                       "avgRating": 1, "imdbId": 1, "txt": {"$slice": 1}}).limit(3):
        pprint.pprint(movie)

# Filter by criteria
def find_movies_by_criteria(criteria) :
    # note: added limit/$slice temporarily
    for movie in collection.find(criteria, {"_id": 1, "item_id": 1, "title": 1, "directedBy": 1, "starring": 1,
                                       "avgRating": 1, "imdbId": 1, "txt": {"$slice": 1}}).limit(1):
        pprint.pprint(movie)

# Movies by rating
def find_movies_by_rating(n, order):
    
    movies = collection.find({}, {"title": 1, "directedBy": 1, "starring": 1, "avgRating": 1})
    movies = movies.sort("avgRating",  order).limit(n)
    
    for movie in movies:
        print(movie)


#Sort movies by criteria
def sort_movies_by_criteria(criteria) :
    # note: added limit temporarily
    for movie in collection.find({}, {"title": 1, "directedBy": 1, "starring": 1, "avgRating": 1}).sort({criteria: 1}).limit(5):
        pprint.pprint(movie)


#Bookmark/un-bookmark movie
def update_bookmark(title, val):
    filter = {"title": {"$regex": "^"+title}}
    update = {"$set": {"bookmarked": val}}
    result = collection.update_many(filter, update, upsert=False)
    print(result)

    for movie in collection.find({"bookmarked": val}, {"title": 1, "avgRating": 1, "bookmarked": 1}).limit(5):
        pprint.pprint(movie)


#Hide/unhide movie
def update_hidden(title, val):
    filter = {"title": {"$regex": "^"+title}}
    update = {"$set": {"hidden": val}}
    result = collection.update_many(filter, update, upsert=False)
    print(result)

    for movie in collection.find({"hidden": val}, {"title": 1, "avgRating": 1, "hidden": 1}).limit(5):
        pprint.pprint(movie)


#Blacklist/whitelist movie
def update_blacklist(title, val):
    filter = {"title": {"$regex": "^"+title}}
    update = {"$set": {"blacklisted": val}}
    result = collection.update_many(filter, update, upsert=False)
    print(result)

    for movie in collection.find({"blacklisted": val}, {"title": 1, "avgRating": 1, "blacklisted": 1}).limit(5):
        pprint.pprint(movie)


#Add a review
def add_movie_review(title, review) :
    movie = collection.find_one({'title': {'$regex': "^" + title}}, {'_id': 1})
    result = collection.update_one({'title': {'$regex': "^" + title}, '_id': movie['_id']}, {'$push': {'txt': review}},
                                   upsert=False)
    print(result)
    movie = collection.find_one({'title': {'$regex': "^" + title}}, {'title': 1, 'txt': 1})
    pprint.pprint(movie)

#Update movie title
def update_title(item_id, new_title):
    myquery = { "item_id": item_id }
    newvalues = { "$set": { "title": new_title } }

    result = collection.update_one(myquery, newvalues)
    print(result)

    movie = collection.find_one({"item_id": item_id}, {'item_id': 1, 'title': 1})
    pprint.pprint(movie)


# Add a new movie
def add_movie(movie):
    result = collection.insert_one(movie)
    print(result)
    movie = collection.find_one({"item_id": movie['item_id']})
    pprint.pprint(movie)

# Delete an existing movie by item_id
def delete_movie(item_id):
    result = collection.delete_one({"item_id": item_id})
    print(result)
    movie = collection.find_one({"item_id": item_id})
    pprint.pprint(movie)

def user_input():
    cont = True
    while cont:
        func = int(input("Which function would you like to call? \n"
                         "1. Find all movies \n"
                         "2. Find movie by item_id \n"
                         "3. Find movie by title \n"
                         "4. Find movie by iMDb id \n"
                         "5. Filter movies by director \n"
                         "6. Filter movies by cast \n"
                         "7. Filter movies by bookmark \n"
                         "8. Filter movies by average rating \n"
                         "9. Filter movies by other criteria \n"
                         "10. Find top movies \n"
                         "11. Find worst movies \n"
                         "12. Sort movies by cast \n"
                         "13. Sort movies by director \n"
                         "14. Sort movies by iMDb id \n"
                         "15. Bookmark movie \n"
                         "16. Un-bookmark movie \n"
                         "17. Hide movie \n"
                         "18. Unhide movie \n"
                         "19. Blacklist movie \n"
                         "20. Whitelist movie \n"
                         "21. Add movie review \n"
                         "22. Add movie \n"
                         "23. Delete movie by item_id \n"
                         "24. Update title for a movie \n"
                         "Type in the number of the function to call : "
                        ))
        if func == 1: find_all_movies()
        elif func == 2:
            item_id = int(input("\nItem_id of the movie to search: "))
            find_movies_by_criteria({'item_id': item_id})
        elif func == 3:
            title = input("\nTitle of the movie to search: ")
            find_movies_by_criteria({'title': {"$regex": "^" + title}})
        elif func == 4:
            imdb_id = int(input("\niMDb_id of the movie to search: "))
            find_movies_by_criteria({'imdbId': imdb_id})
        elif func == 5:
            directed_by = input("\nDirector of the movie to search: ")
            find_movies_by_criteria({"directedBy": {"$regex": "^" + directed_by}})
        elif func == 6:
            cast = input("\nCast of the movie to search: ")
            find_movies_by_criteria({'starring': {"$regex": "^"+cast}})
        elif func == 7:
            find_movies_by_criteria({'bookmarked': True})
        elif func == 8:
            avg_rating = input("\nAverage rating of the movie to search: ")
            find_movies_by_criteria({'avgRating': float(avg_rating)})
        elif func == 9:
            criteria = {}
            item_id = input("\nItem Id? (n to skip)")
            if item_id != "n": criteria['item_id'] = int(item_id)
            title = input("\nTitle? (n to skip)")
            if title != "n": criteria['title'] = {"$regex": "^" + title}
            directed_by = input("\nDirector? (n to skip)")
            if directed_by != "n": criteria['directedBy'] = {"$regex": "^" + directed_by}
            avg_rating = input("\nAverage rating? (n to skip)")
            if avg_rating != "n": criteria['avgRating'] = float(avg_rating)
            cast = input("\nCast? (n to skip)")
            if cast != "n": criteria['starring'] = {"$regex": "^" + cast}
            imdbId = input("\niMDb id? (n to skip)")
            if imdbId != "n": criteria['imdbId'] = int(imdbId)
            find_movies_by_criteria(criteria)
        elif func == 10:
            n = int(input("\nTop n movies (value for n): "))
            find_movies_by_rating(n, DESCENDING)
        elif func == 11:
            n = int(input("\nWorst n movies (value for n): "))
            find_movies_by_rating(n, ASCENDING)
        elif func == 12:
            sort_movies_by_criteria('starring')
        elif func == 13:
            sort_movies_by_criteria('directedBy')
        elif func == 14:
            sort_movies_by_criteria('imdbId')
        elif func == 15:
            title = input("\nTitle of the movie to bookmark: ")
            update_bookmark(title, True)
        elif func == 16:
            title = input("\nTitle of the movie to un-bookmark: ")
            update_bookmark(title, False)
        elif func == 17:
            title = input("\nTitle of the movie to hide: ")
            update_hidden(title, True)
        elif func == 18:
            title = input("\nTitle of the movie to unhide: ")
            update_hidden(title, False)
        elif func == 19:
            title = input("\nTitle of the movie to blacklist: ")
            update_blacklist(title, True)
        elif func == 20:
            title = input("\nTitle of the movie to whitelist: ")
            update_blacklist(title, False)
        elif func == 21:
            title = input("\nTitle of the movie to review: ")
            review = input("\nReview to add: ")
            add_movie_review(title, review)
        elif func == 22:
            item_id = int(input("\nMovie item_id: "))
            title = input("\nMovie title: ")
            directed_by = input("\nMovie director: ")
            cast = input("\nMovie cast: ")
            imdb_id = int(input("\nMovie iMDb id: "))
            avg_rating = float(input("\nMovie average rating: "))
            movie = {'item_id': item_id, 'title': title, 'directedBy': directed_by, 'starring': cast, 'imdbId': imdb_id,
                     'avgRating': avg_rating}
            add_movie(movie)
        elif func == 23:
            item_id = input("\nItem_id of the movie to delete: ")
            delete_movie(item_id)
        elif func == 24:
            item_id = input("\nItem_id of the movie to update: ")
            new_title = input("\nUpdated title: ")
            update_title(item_id, new_title)

        else: print("\nPlease input a valid operation number: ")

        cont = (input("Continue? (y/n)") == 'y')

print("Connected to Mongo movie database.\n")
user_input()