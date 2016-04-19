# Matthew Sudmann-Day
# Barcelona GSE Data Science
# Basic MapReduce example

from mrjob.job import MRJob

class MR_Weblog_Distiller(MRJob):

	def mapper(self, _, line):
		# Split the line into words with the separator being the space character.
		words = line.split()

		# Some lines in the file cannot be parsed and cause errors.  This
		# condition enables us to skip the lines that do not have the correct
		# format.
		if len(words) >= 7:

			# Extract the requesting IP address, the date (excluding time)
			# and the site that was visited.
			ip = words[0]
			dt = words[3][1:12]
		        site = words[6]

			# Output the IP and SITE fields as keys, and the date being the
			# value for both.
			yield "IP:" + ip, dt
			yield "SITE:" + site, dt

	def reducer(self, key, values):

		# The values contain the dates for the IP/SITE key.  We consider multiple
		# visits with the same date as being a single visit.  Hence, we need to
		# count the unique list of dates.  The set() function reduces the dates to
		# a unique list and len() counts them.
		yield key, len(set(values))

if __name__ == '__main__':
	MR_Weblog_Distiller.run()