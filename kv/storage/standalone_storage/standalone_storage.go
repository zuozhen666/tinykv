package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	//added packages
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//How to initialise engine depends on config.
	kv := engine_util.CreateDB("kv", conf)
	raft := engine_util.CreateDB("raft", conf)
	kvPath := filepath.Join(conf.DBPath, "kv")
	raftPath := filepath.Join(conf.DBPath, "raft")

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kv, raft, kvPath, raftPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//For read-only transactions,set update(parameter) to false.
	kvTxn := s.engine.Kv.NewTransaction(false)
	raftTxn := s.engine.Raft.NewTransaction(false)
	return &StandAloneStorageReader{
		kvTxn: kvTxn,
		raftTxn: raftTxn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// op is a kind of interface{} whitch includes storage.Put and storage.Delete.
	for _, op := range batch {
		switch op.Data.(type) {
		case storage.Put:
			put := op.Data.(storage.Put)
			var txn *badger.Txn
			if put.Cf == "kv" {
				txn = s.engine.Kv.NewTransaction(true)
			} else {
				txn = s.engine.Raft.NewTransaction(true)
			}
			//Set adds a key-value pair to the database.
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
			//If error is nil,the transaction is successfully committed.
			err = txn.Commit()
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := op.Data.(storage.Delete)
			var txn *badger.Txn
			if delete.Cf == "kv" {
				txn = s.engine.Kv.NewTransaction(true)
			} else {
				txn = s.engine.Raft.NewTransaction(true)
			}
			//Delete deletes a key.
			err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			if err != nil {
				return err
			}
			//If error is nil,the transaction is successfully committed.
			err = txn.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	kvTxn *badger.Txn
	raftTxn *badger.Txn
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var txn *badger.Txn
	if cf == "kv" {
		txn = reader.kvTxn
	} else {
		txn = reader.raftTxn
	}
	//val depends on cf and key.
	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	var txn *badger.Txn
	if cf == "kv" {
		txn = reader.kvTxn
	} else {
		txn = reader.raftTxn
	}
	//A struct includes a new iterator and a prefix.
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.kvTxn.Discard()
	reader.raftTxn.Discard()
}
