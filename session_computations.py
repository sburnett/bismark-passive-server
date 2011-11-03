from abc import abstractmethod, abstractproperty, ABCMeta
import gzip
from os.path import join
import tarfile
from update_parser import PassiveUpdate

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

    def process_session(self, update_files, updates_directory):
        processed_new_update = False
        session_result = None
        current_tarname = None
        for tarname, filename in update_files:
            if filename in self._filenames_processed:
                print ' ', filename, 'skipped (filename)'
                continue
            if current_tarname != tarname:
                print tarname
                current_tarname = tarname
                full_tarname = join(updates_directory, current_tarname)
                tarball = tarfile.open(full_tarname, 'r')
            print ' ', filename,
            tarhandle = tarball.extractfile(filename)
            update_content = gzip.GzipFile(fileobj=tarhandle).read()
            update = PassiveUpdate(update_content)
            if update.sequence_number == self._last_sequence_number_processed + 1:
                update_result = self.process_update(update)
                session_result = self.augment_session_result(session_result,
                                                             update_result)
                self._last_sequence_number_processed = update.sequence_number
                self._filenames_processed.add(filename)
                print 'processed'
            else:
                print 'skipped (sequence number)'
            processed_new_update = True
        return processed_new_update, session_result

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
