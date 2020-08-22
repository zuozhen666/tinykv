package standalone_storage

import (
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB("kv", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// op is a kind of interface{} whitch includes storage.Put and storage.Delete.
	for _, op := range batch {
		switch op.Data.(type) {
		case storage.Put:
			put := op.Data.(storage.Put)
			txn := s.db.NewTransaction(true)
			// Set adds a key-value pair to the database.
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
			// If error is nil, the transaction is successfully committed.
			if err := txn.Commit(); err != nil {
				return err
			}
		case storage.Delete:
			delete := op.Data.(storage.Delete)
			txn := s.db.NewTransaction(true)
			// Delete deletes a key.
			if err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key)); err != nil {
				return err
			}
			// If error is nil, the transaction is successfully committed.
			if err := txn.Commit(); err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}
