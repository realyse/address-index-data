"""
ONS Address Index - Probabilistic Parser
========================================

This file defines the calling mechanism for a trained probabilistic parser model.


Requirements
------------

:requires: pycrfsuite (https://python-crfsuite.readthedocs.io/en/latest/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 20-Oct-2016
"""
import ProbabilisticParser.common.tokens as t
import pycrfsuite
from collections import OrderedDict
import sys
import warnings


try:
    TAGGER = pycrfsuite.Tagger()
    TAGGER.open(t.MODEL_PATH + t.MODEL_FILE)
    print('Using model from', t.MODEL_PATH + t.MODEL_FILE)
except IOError:
    TAGGER = None
    warnings.warn('You must train the model to create the %s file before you can use the parse and tag methods' % t.MODEL_FILE)
    sys.exit(-9)


def parse(raw_string):
    """
    Parse the given input string using a trained model. Returns a list of tokens and labels.

    :param raw_string: input string to parse
    :type raw_string: str

    :return: a list of tokens and labels
    :rtype: list
    """

    tokens = t.tokenize(raw_string)
    if not tokens:
        return []

    features = t.tokens2features(tokens)

    tags = TAGGER.tag(features)

    return list(zip(tokens, tags))


def tag(raw_string):
    """
    Parse the given input string using a trained model. Returns an ordered dictionary of tokens and labels.
    Unlike the parse function returns a complete label i.e. joins multiple labels to a single string and
    labels the full string given the label.

    :param raw_string: input string to parse and label
    :type raw_string: str

    :return: a dictionary of tokens and labels
    :rtype: Ordered Dictionary
    """
    tagged = OrderedDict()

    for token, label in parse(raw_string):
        tagged.setdefault(label, []).append(token)

    for token in tagged:
        component = ' '.join(tagged[token])
        component = component.strip(' ,;')
        tagged[token] = component

    return tagged


def debugging(raw_string='LTD'):
    import os

    tokens = t.tokenize(raw_string)
    features = t.tokens2features(tokens)
    # print('features:', features)

    tags = TAGGER.tag(features)
    print('tags:', tags)

    print(TAGGER.probability(tags))
    print(TAGGER.marginal(tags[0], 0))

    # print(TAGGER.info().transitions)
    # print(TAGGER.info().state_features)
    # print(TAGGER.info().attributes)

    tmp = pycrfsuite.ItemSequence(features)
    items = tmp.items()[0]
    print(items)

    # write to a text file
    fh = open('training/test.txt', 'w')
    fh.write(tags[0] + '\t')
    for item in sorted(items):
        if 'digits' in item or 'length' in item or 'word' in item:
            fh.write(str(item) + '=' + str(items[item]) + '\t')
        else:
            fh.write(str(item) + ':' + str(items[item]) + '\t')
    fh.write('\n')
    fh.close()

    # command line call to the C code to test the output
    os.system('crfsuite tag -pit -m training/addressCRF.crfsuite training/test.txt')


if __name__ == "__main__":
    debugging()

    # debugging(raw_string='ARBITRARY')
