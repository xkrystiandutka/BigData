from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(mapper=self.mapperGetKeys,
                   reducer=self.reducerMostCommonWord)
        ]

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

    def mapperGetKeys(self, key, value):
        yield None, (value, key)

    def reducerMostCommonWord(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MRJobFirstStep.run()


