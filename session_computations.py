from abc import abstractmethod, abstractproperty, ABCMeta
import cPickle
import errno
import glob
import gzip
import os.path

import update_parser

class SessionProcessor(object):
    """This class is instantiated once per bismark-passive client session.
    (i.e., once per bottom-level subdirectory of the index.)

    IMPORTANT: Subclasses must be serializable with the pickle module!"""

    __metaclass__ = ABCMeta

    def __init__(self):
        """Be sure to call this constructor in your subclass."""
        self._filenames_processed = set()
        self._last_sequence_number_processed = -1

    @abstractmethod
    def process_update(self, update):
        """Process a single update. This method is guaranteed to be called with
        updates with sequence numbers incrementing from 0."""

    @abstractproperty
    def results(self):
        """Return a dictionary of computation results. This will be called
        after the final call to process_update."""

    @abstractproperty
    def augment_session_result(self, session_result, update_result):
        """The session result is a value returned as the result of processing
        an entire session directory, while the update result is the result of
        processing a single update. This method should merge the two to
        produce a sensible session result. For example, if process_update
        returns a count of the number of bytes in the processed update, then
        this method could sum the update results into the session result to
        produce a count of the total number of bytes processed in the entire
        session.

        session_result will be None the first time this is called. This
        method should return the new value of session_result.

        IMPORTANT: This method is only run once per update. It is not run
        when results are load from the pickle cache."""

    def process_session(self, session_dir):
        print session_dir
        processed_new_update = False
        session_result = None
        filenames = glob.glob(os.path.join(session_dir, '*.gz'))
        filenames.sort(key=lambda f: os.path.getmtime(f))
        for filename in filenames:
            basename = os.path.basename(filename)
            print ' ', basename,
            if basename in self._filenames_processed:
                print 'skipped (filename)'
                continue
            update_content = gzip.open(filename).read()
            update = update_parser.PassiveUpdate(update_content)
            if update.sequence_number \
                    == self._last_sequence_number_processed + 1:
                update_result = self.process_update(update)
                session_result = self.augment_session_result(session_result,
                                                             update_result)
                self._last_sequence_number_processed = update.sequence_number
                print 'processed'
            else:
                print 'skipped (sequence number)'
            self._filenames_processed.add(basename)
            processed_new_update = True
        return (processed_new_update, session_result)

class SessionAggregator(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def augment_results(self,
                        node_id,
                        anonymization_id,
                        session_id,
                        results,
                        updated,
                        process_result):
        """Process results from a computation. It will be called many times,
        and results will arrive in no particular order."""

    @abstractmethod
    def store_results(self):
        """Do something with the results collected by augment_results. It will
        be called after the last call to augment_results."""

def merge_timeseries(first, second):
    for key, value in first.items():
        second[key] += value

def process_sessions(computations, index_directory, pickle_root_directory):
    for nodedir in glob.glob(os.path.join(index_directory, '*')):
        if not os.path.isdir(nodedir):
            continue

        session_dirs = glob.glob(os.path.join(nodedir, '*', '*'))
        session_dirs.sort(key=lambda f: os.path.basename(f))
        for session_dir in session_dirs:
            if not os.path.isdir(session_dir):
                continue

            pickle_subdir = os.path.join(pickle_root_directory,
                                         os.path.relpath(session_dir,
                                                         index_directory))
            try:
                os.makedirs(pickle_subdir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            for processor_class, aggregator in computations:
                pickle_filename = '%s.pickle' % processor_class.__name__
                pickle_path = os.path.join(pickle_subdir, pickle_filename)
                try:
                    processor = cPickle.load(open(pickle_path, 'rb'))
                except:
                    processor = processor_class()
                processed_updates, process_result = processor.process_session(session_dir)
                if processed_updates:
                    cPickle.dump(processor, open(pickle_path, 'wb'), 2)

                node_id = os.path.relpath(nodedir, index_directory)
                anonymized_id, session_id \
                        = os.path.split( os.path.relpath(session_dir, nodedir))
                aggregator.augment_results(node_id,
                                           anonymized_id,
                                           session_id,
                                           processor.results,
                                           processed_updates,
                                           process_result)

    for _, aggregator in computations:
        aggregator.store_results()
