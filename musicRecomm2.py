from mrjob.job import MRJob

#Change in Branch new Spike
class MusicRecommendationSystem(MRJob):
	def steps(self):
	 	return[
	 			self.mr(mapper=self.mapper_get_tracks_ratings,
	 				reducer=self.reducer_group_user_rating),
	 			self.mr(mapper=self.mapper_get_user_combinations,
	 				reducer=self.reducer_aggregate),
	 			self.mr(mapper=self.mapper_splitter,
	 				reducer=self.reducer_aggregate1)
	 			
	 					
			]
	def mapper_get_tracks_ratings(self,key,line):
		
		user_id, track_id, rating=line.split('\t')
		user_ratings=[]
		user_ratings.append(user_id)
		user_ratings.append(float(rating))
		yield track_id,user_ratings
		#Change in Branch new Spike
	def reducer_group_user_rating(self,track_id,user_ratings):
		UserRatingList = []
		for ur in user_ratings:
			UserRatingList.append((ur))
		yield track_id,(UserRatingList)
	def mapper_get_user_combinations(self,track_id,UserRatingList):
		UserRatingListPairs=UserRatingList
		

		for combination1, combination2 in combinations(UserRatingListPairs,2):
			UserList1=[]
			SimilarUsers1=[]
			User_sel1=[]
			UserFav1=[]

			UserList2=[]
			SimilarUsers2=[]
			User_sel2=[]
			UserFav2=[]
			if combination1[1]==combination2[1]:
				SimilarUsers1.append((combination2[0],1))
				SimilarUsers2.append((combination1[0],1))
				if combination1[1]==5:
					UserFav1.append(track_id)
					UserFav2.append(track_id)

			else:
				if combination1[1]==5:
					UserFav1.append(track_id)
				else:
					User_sel1.append(track_id)
				if combination2[1]==5:
					UserFav2.append(track_id)
				else:
					User_sel2.append(track_id)

			UserList1.append((SimilarUsers1))
			UserList1.append((User_sel1))
			UserList1.append((UserFav1))

			UserList2.append((SimilarUsers2))
			UserList2.append((User_sel2))
			UserList2.append((UserFav2))
			yield combination1[0],(UserList1)
			yield combination2[0],(UserList2)

	def reducer_aggregate(self,user_id,UserList):
		UserList1=[]
		SimilarUsers1=[]
		User_sel1=[]
		UserFav1=[]
		RecommTracks=[]
		for SimilarUsers,User_sel,UserFav in UserList:
			'''			'''
			if len(SimilarUsers) > 0:
				if len(SimilarUsers1) > 0:
					i=0;
					y=len(SimilarUsers1)
					while i < y:
		
						if SimilarUsers1[i][0] == SimilarUsers[0][0]:
							SimilarUsers1[i][1]+=1
							break
						i=i+1
					if i==y:
						SimilarUsers1.append((SimilarUsers[0]))
				else:
					SimilarUsers1.append((SimilarUsers[0]))
			''' 		'''
					
			if len(User_sel) > 0:
				if len(User_sel1) > 0:
					i=0;
					y=len(User_sel1)
					while i < y:	
						
						if User_sel1[i] == User_sel[0]:
							break
						i+=1
					if i==y:
						User_sel1.append(User_sel[0])
				else:
					User_sel1.append(User_sel[0])
			''' 		'''
			if len(UserFav) > 0:
				if len(UserFav1) > 0:
					i=0;
					y=len(UserFav1)
					while i < y:	
						
						if UserFav1[i] == UserFav[0]:
							break
						i+=1
					if i==y:
						UserFav1.append(UserFav[0])
				else:
					UserFav1.append(UserFav[0])

			
		UserList1.append((SimilarUsers1))
		UserList1.append(User_sel1)
		UserList1.append(UserFav1)
		UserList1.append(RecommTracks)
		yield user_id,(SimilarUsers1,User_sel1,UserFav1,RecommTracks)

	def mapper_splitter(self,user_id, UserList):
		UserList1=[]

		SimilarUsersn=[]
		User_seln=[]
		UserFavn=[]
		RecommTracks=[]
	
		for data in UserList[0]:
			UserList1=[]
			UserList1.append((SimilarUsersn))
			UserList1.append(User_seln)
			
			UserList1.append(UserFavn)
			for track in UserList[2]:
				RecommTracks.append((track,data[1]))
			UserList1.append(RecommTracks)
			yield data[0],(UserList1)
		
		yield user_id,(UserList)


	def reducer_aggregate1(self,user_id,UserList):
		finalList=[]
		
		User_seln=[]
		UserFavn=[]
		RecommTracks=[]
		SimilarUsersn=[]
		RecommTracks1=[]
		for UserListfinal in UserList:
			RecommTracksList=UserListfinal[3]
	
			print UserListfinal[3]
			if len(UserListfinal[0]) > 0:
				SimilarUsersn.append(UserListfinal[0])
			if len(UserListfinal[1]) > 0:
				User_seln.append(UserListfinal[1])
			if len(UserListfinal[2]) > 0:
				UserFavn.append(UserListfinal[2])
		
			if len(RecommTracksList) > 0:
				if len(RecommTracks) > 0:
			
					y=len(RecommTracks) 
					for x in RecommTracksList:
				
						i=0
						while i < y:
					
							if RecommTracks[i][0] == x[0]:
								RecommTracks[i][1]=RecommTracks[i][1]+x[1]
								break 
							i=i+1
						if i==y:
							RecommTracks.append((x))
				else:
					RecommTracks=RecommTracksList

		finalList.append(SimilarUsersn)
		finalList.append(User_seln)
		finalList.append(UserFavn)
		finalList.append(RecommTracks)
		yield user_id,(finalList)




def combinations(iterable, r):
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = range(r)
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i + 1, r):
            indices[j] = indices[j - 1] + 1
        yield tuple(pool[i] for i in indices)

	

if __name__ == '__main__':
 	MusicRecommendationSystem.run()
