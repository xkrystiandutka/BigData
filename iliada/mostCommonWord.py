from mrjob.job import MRJob
from mrjob.step import MRStep
import  re

WORD_RE = re.compile(r'[\w]+')

class MRMostCommonWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapperGetWords,
                   reducer=self.reducerCountWords),
            MRStep(mapper=self.mapperGetKeys,
                   reducer=self.reducerFindMostCommonWord)
        ]

    def mapperGetWords(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducerCountWords(self, word, counts):
        yield word, sum(counts)

    def mapperGetKeys(self, key, value):
        yield None, (value, key)

    def reducerFindMostCommonWord(self, key, values):
        yield  max(values)

if __name__ == '__main__':
    MRMostCommonWord.run()