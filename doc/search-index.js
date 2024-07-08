var searchIndex = new Map(JSON.parse('[\
["pravega_client",{"t":"CCCCCCFFNNNNNNNNNNNNNNNNNNNCONNNNNNNNCFNNNNNNOFNNNNNNNFFNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNPGPPNNNNNNNNNNNNNNNNNNNNNOOOFFFFFNNNNNNNNNNONNNNNNNNNNNNNNNNNNNNNNNONNNNNNNNNNNNNNNOCCCCNNNNNNNNNNNNNNNNNNNNCFFGFFPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNONNNNNNNNNNONNNNNNNNNNNNNNOONOOONNNONONNNNNNNNNNNNNNONNNNOPFFFGFGPPPNNNNNNNNNNNNNNNNNNNNNNNNONNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNONNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOOFPFGPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNONNNNNNNNNNNNNNNNNNNOOOOPFGFGPPPPPPPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOOOOOOOOOOFNNNKFFSKNNNNNNNNNNNNMNNNNNNNCNNNNNNMNNCFPFGFPFPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOOONNNNNNNNNNNNNNNNNNNNNNNNOOOSFGFPFPFPFPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOOONNNNNNNNNNNNNNNNNNNNNNNNNNNOOOOOFFNNNNNNNNNNNNNNNNNNCCNNNNNNNNFFFPPPPFGSFFKKKNNNNNNNNNNNNNNNNNMNNNNNONNNNNNNNNNNNNNHNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOONNNNNHNMNNNNNNNNNNNNNNNNNNNNNNNONNNNNNOOOOOPPPPFPGINNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNOOOOOOOOOOO","n":["byte","client_factory","error","event","index","sync","ByteReader","ByteWriter","borrow","borrow","borrow_mut","borrow_mut","deref","deref","deref_mut","deref_mut","drop","drop","drop","from","from","init","init","into","into","into_request","into_request","reader","segment","try_from","try_from","try_into","try_into","type_id","type_id","vzip","vzip","writer","ByteReader","available","current_head","current_offset","current_tail","read","seek","segment","ByteWriter","current_offset","flush","reset","seal","seek_to_tail","truncate_data_before","write","ClientFactory","ClientFactoryAsync","borrow","borrow","borrow_mut","borrow_mut","clone","clone_into","config","config","controller_client","controller_client","create_byte_reader","create_byte_reader","create_byte_writer","create_byte_writer","create_event_writer","create_event_writer","create_index_reader","create_index_reader","create_index_writer","create_index_writer","create_reader_group","create_reader_group","create_reader_group_with_config","create_reader_group_with_config","create_stream_meta_client","create_synchronizer","create_synchronizer","create_table","create_table","create_transactional_event_writer","create_transactional_event_writer","delete_reader_group","delete_reader_group","deref","deref","deref_mut","deref_mut","drop","drop","fmt","from","from","init","init","into","into","into_request","into_request","new","new","new_with_runtime","runtime","runtime_handle","runtime_handle","to_async","to_owned","try_from","try_from","try_into","try_into","type_id","type_id","vzip","vzip","ConditionalCheckFailure","Error","InternalFailure","InvalidInput","as_error_source","backtrace","borrow","borrow_mut","cause","deref","deref_mut","description","drop","fmt","fmt","from","init","into","into_request","source","to_string","try_from","try_into","type_id","vzip","msg","msg","msg","EventReader","EventWriter","ReaderGroup","Transaction","TransactionalEventWriter","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","config","deref","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","deref_mut","drop","drop","drop","drop","drop","drop","drop","drop","from","from","from","from","from","id","init","init","init","init","init","into","into","into","into","into","into_request","into_request","into_request","into_request","into_request","name","reader","reader_group","reader_group_state","transactional_writer","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","vzip","vzip","vzip","vzip","vzip","writer","Event","EventReader","EventReaderError","SegmentSlice","SliceMetadata","StateError","acquire_segment","as_error_source","backtrace","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","cause","clone","clone_into","default","default","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","description","drop","drop","drop","drop","drop","end_offset","fmt","fmt","fmt","fmt","fmt","from","from","from","from","has_events","id","init","init","init","init","into","into","into","into","into_iter","into_request","into_request","into_request","into_request","is_empty","last_event_offset","meta","next","offset_in_segment","partial_data_present","read_offset","reader_offline","release_segment","release_segment_at","scoped_segment","source","start_offset","to_owned","to_string","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","value","vzip","vzip","vzip","vzip","source","Cbor","ReaderGroup","ReaderGroupConfig","ReaderGroupConfigBuilder","SerdeError","StreamCutV1","StreamCutVersioned","Tail","Unbounded","V1","add_stream","as_error_source","backtrace","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","build","cause","clone","clone","clone","clone_box","clone_box","clone_box","clone_into","clone_into","clone_into","config","create_reader","default","deref","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","deref_mut","description","deserialize","deserialize","deserialize","drop","drop","drop","drop","drop","eq","eq","eq","fmt","fmt","fmt","fmt","fmt","from","from","from","from","from","from_bytes","get_managed_streams","get_positions","get_start_stream_cuts","get_stream","get_streamcut","get_streams","init","init","init","init","init","into","into","into","into","into","into_request","into_request","into_request","into_request","into_request","list_readers","name","new","new","read_from_head_of_stream","read_from_stream","read_from_tail_of_stream","reader_offline","serialize","serialize","serialize","serialize_value","serialize_value","serialize_value","set_group_refresh_time","source","to_bytes","to_owned","to_owned","to_owned","to_string","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","vzip","vzip","vzip","vzip","vzip","msg","source","Offset","ReaderAlreadyOfflineError","ReaderGroupState","ReaderGroupStateError","SyncError","add_reader","as_error_source","assign_segment_to_reader","backtrace","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","cause","check_online","clone","clone_box","clone_into","compute_segments_to_acquire_or_release","deref","deref","deref","deref_mut","deref_mut","deref_mut","description","deserialize","drop","drop","drop","eq","equivalent","equivalent","equivalent","equivalent","fmt","fmt","fmt","from","from","from","get_hash","get_online_readers","get_segments","get_segments_for_reader","get_streamcut","hash","init","init","init","into","into","into","into_request","into_request","into_request","is_precondition","new","read","release_segment","segment_completed","serialize","serialize_value","source","to_owned","to_string","try_from","try_from","try_from","try_into","try_into","try_into","type_id","type_id","type_id","vzip","vzip","vzip","error_msg","error_msg","source","source","PingerError","Transaction","TransactionError","TransactionalEventWriter","TransactionalEventWriterError","TxnAbortError","TxnClosed","TxnCommitError","TxnControllerError","TxnSegmentWriterError","TxnStreamControllerError","TxnStreamWriterError","abort","as_error_source","as_error_source","backtrace","backtrace","begin","borrow","borrow","borrow_mut","borrow_mut","cause","cause","check_status","commit","deref","deref","deref_mut","deref_mut","description","description","drop","drop","fmt","fmt","fmt","fmt","from","from","get_txn","init","init","into","into","into_request","into_request","source","source","stream","to_string","to_string","try_from","try_from","try_into","try_into","txn_id","type_id","type_id","vzip","vzip","write_event","error_msg","id","id","id","source","source","status","status","msg","source","EventWriter","flush","write_event","write_event_by_routing_key","Fields","IndexReader","IndexWriter","RECORD_SIZE","Value","borrow","borrow","borrow_mut","borrow_mut","deref","deref","deref_mut","deref_mut","drop","drop","from","from","get_field_values","get_record_size","init","init","into","into","into_request","into_request","reader","try_from","try_from","try_into","try_into","type_id","type_id","value","vzip","vzip","writer","FieldNotFound","FieldNotFound","IndexReader","IndexReaderError","Internal","Internal","InvalidOffset","InvalidOffset","as_error_source","backtrace","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","build","build","build","cause","clone","clone","clone","clone_into","clone_into","clone_into","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","description","drop","drop","drop","drop","fail","fail","fail","first_record_data","fmt","fmt","fmt","fmt","fmt","from","from","from","from","head_offset","init","init","init","init","into","into","into","into","into_error","into_error","into_error","into_request","into_request","into_request","into_request","last_record_data","msg","msg","msg","read","search_offset","source","tail_offset","to_owned","to_owned","to_owned","to_string","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","vzip","vzip","vzip","vzip","msg","msg","msg","INDEX_RECORD_SIZE_ATTRIBUTE_ID","IndexWriter","IndexWriterError","Internal","Internal","InvalidCondition","InvalidCondition","InvalidData","InvalidData","InvalidFields","InvalidFields","append","append_conditionally","as_error_source","backtrace","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","build","build","build","cause","clone","clone","clone","clone","clone_into","clone_into","clone_into","clone_into","deref","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","deref_mut","description","drop","drop","drop","drop","drop","fail","fail","fail","flush","fmt","fmt","fmt","fmt","fmt","fmt","from","from","from","from","from","init","init","init","init","init","into","into","into","into","into","into_error","into_error","into_error","into_error","into_request","into_request","into_request","into_request","into_request","msg","msg","msg","source","to_owned","to_owned","to_owned","to_owned","to_string","truncate","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","vzip","vzip","vzip","vzip","vzip","backtrace","msg","msg","msg","source","Synchronizer","Table","borrow","borrow","borrow_mut","borrow_mut","deref","deref","deref_mut","deref_mut","drop","drop","from","from","init","init","into","into","into_request","into_request","synchronizer","table","try_from","try_from","try_into","try_into","type_id","type_id","vzip","vzip","Insert","Key","Remove","SyncPreconditionError","SyncTableError","SyncTombstoneError","SyncUpdateError","Synchronizer","SynchronizerError","TOMBSTONE","Update","Value","ValueClone","ValueData","ValueSerialize","as_error_source","backtrace","borrow","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","cause","clone","clone","clone_box","clone_box","clone_into","clone_into","contains_key","contains_outer_key","data","deref","deref","deref","deref","deref","deref","deref_mut","deref_mut","deref_mut","deref_mut","deref_mut","deref_mut","description","deserialize","deserialize_from","drop","drop","drop","drop","drop","drop","eq","eq","equivalent","equivalent","equivalent","equivalent","equivalent","equivalent","equivalent","equivalent","fetch_updates","fmt","fmt","fmt","fmt","from","from","from","from","from","from","get","get","get_hash","get_inner_map","get_inner_map","get_key_version","get_name","get_outer_map","hash","init","init","init","init","init","init","insert","insert","insert_tombstone","into","into","into","into","into","into","into_request","into_request","into_request","into_request","into_request","into_request","is_empty","key","key_version","new","new","new","remove","retain","serialize","serialize","serialize_value","serialize_value","source","to_owned","to_owned","to_string","try_from","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","type_id","type_id","vzip","vzip","vzip","vzip","vzip","vzip","error_msg","error_msg","error_msg","operation","source","ConnectionError","IncorrectKeyVersion","KeyDoesNotExist","OperationError","Table","TableDoesNotExist","TableError","Version","as_error_source","backtrace","borrow","borrow_mut","cause","deref","deref_mut","description","drop","fmt","fmt","from","get","get_all","init","insert","insert_all","insert_conditionally","insert_conditionally_all","into","into_request","read_entries_stream","read_entries_stream_from_position","read_keys_stream","remove","remove_all","remove_conditionally","remove_conditionally_all","source","to_string","try_from","try_into","type_id","vzip","can_retry","error_msg","error_msg","error_msg","name","operation","operation","operation","operation","operation","source"],"q":[[0,"pravega_client"],[6,"pravega_client::byte"],[38,"pravega_client::byte::reader"],[46,"pravega_client::byte::writer"],[54,"pravega_client::client_factory"],[120,"pravega_client::error"],[145,"pravega_client::error::Error"],[148,"pravega_client::event"],[229,"pravega_client::event::reader"],[322,"pravega_client::event::reader::EventReaderError"],[323,"pravega_client::event::reader_group"],[455,"pravega_client::event::reader_group::SerdeError"],[457,"pravega_client::event::reader_group_state"],[537,"pravega_client::event::reader_group_state::ReaderGroupStateError"],[541,"pravega_client::event::transactional_writer"],[603,"pravega_client::event::transactional_writer::TransactionError"],[611,"pravega_client::event::transactional_writer::TransactionalEventWriterError"],[613,"pravega_client::event::writer"],[617,"pravega_client::index"],[653,"pravega_client::index::reader"],[751,"pravega_client::index::reader::IndexReaderError"],[754,"pravega_client::index::writer"],[871,"pravega_client::index::writer::IndexWriterError"],[876,"pravega_client::sync"],[906,"pravega_client::sync::synchronizer"],[1058,"pravega_client::sync::synchronizer::SynchronizerError"],[1063,"pravega_client::sync::table"],[1105,"pravega_client::sync::table::TableError"],[1116,"tonic::request"],[1117,"core::result"],[1118,"core::any"],[1119,"std::io::error"],[1120,"std::io"],[1121,"pravega_client_config"],[1122,"pravega_controller_client"],[1123,"pravega_client_shared"],[1124,"core::cmp"],[1125,"core::fmt"],[1126,"alloc::string"],[1127,"tokio::runtime::handle"],[1128,"tokio::runtime::runtime"],[1129,"core::error"],[1130,"snafu::backtrace_inert"],[1131,"core::option"],[1132,"alloc::boxed"],[1133,"serde::de"],[1134,"alloc::vec"],[1135,"std::collections::hash::map"],[1136,"serde::ser"],[1137,"serde_cbor::ser"],[1138,"serde_cbor::error"],[1139,"core::hash"],[1140,"core::marker"],[1141,"std::collections::hash::set"],[1142,"im::hash::map"],[1143,"tokio::sync::oneshot"],[1144,"core::convert"],[1145,"core::clone"],[1146,"futures_core::stream"],[1147,"core::ops::function"]],"i":[0,0,0,0,0,0,0,0,3,7,3,7,3,7,3,7,3,3,7,3,7,3,7,3,7,3,7,0,7,3,7,3,7,3,7,3,7,0,0,7,7,7,7,7,7,7,0,3,3,3,3,3,3,3,0,0,17,16,17,16,16,16,17,16,17,16,17,16,17,16,17,16,17,16,17,16,17,16,17,16,16,17,16,17,16,17,16,17,16,17,16,17,16,17,16,16,17,16,17,16,17,16,17,16,17,16,17,17,17,16,17,16,17,16,17,16,17,16,17,16,14,0,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,14,115,116,117,0,0,0,0,0,45,29,35,46,21,45,29,35,46,21,29,45,29,35,46,21,45,29,35,46,21,45,45,29,35,46,46,21,21,45,29,35,46,21,45,45,29,35,46,21,45,29,35,46,21,45,29,35,46,21,29,0,0,0,0,45,29,35,46,21,45,29,35,46,21,45,29,35,46,21,45,29,35,46,21,0,0,0,0,0,0,48,45,48,48,48,50,47,49,48,50,47,49,48,49,49,47,49,48,50,47,49,48,50,47,49,48,48,50,47,47,49,49,48,48,50,47,49,48,50,47,49,49,45,48,50,47,49,48,50,47,49,47,48,50,47,49,47,49,47,47,50,49,49,45,45,45,49,48,49,49,48,48,50,47,49,48,50,47,49,48,50,47,49,50,48,50,47,49,118,53,0,0,0,0,0,0,54,54,54,52,53,53,52,30,54,55,53,52,30,54,55,53,52,53,30,54,55,30,54,55,30,54,55,29,29,52,52,30,54,55,53,52,30,54,55,53,53,30,54,55,52,30,54,55,53,30,54,55,30,54,55,53,53,52,30,54,55,53,30,29,55,30,55,29,30,52,30,54,55,53,52,30,54,55,53,52,30,54,55,53,29,29,30,55,52,52,52,29,30,54,55,30,54,55,52,53,30,30,54,55,53,52,30,54,55,53,52,30,54,55,53,52,30,54,55,53,52,30,54,55,53,119,119,0,64,0,0,64,68,64,68,64,68,64,69,68,64,69,64,68,69,69,69,68,68,64,69,68,64,69,64,69,68,64,69,69,69,69,69,69,64,64,69,68,64,69,69,68,68,68,68,69,68,64,69,68,64,69,68,64,69,64,69,69,68,68,69,69,64,69,64,68,64,69,68,64,69,68,64,69,68,64,69,120,121,120,121,82,0,0,0,0,81,81,81,81,81,82,81,46,82,81,82,81,35,82,81,82,81,82,81,46,46,82,81,82,81,82,81,82,81,82,82,81,81,82,81,35,82,81,82,81,82,81,82,81,46,82,81,82,81,82,81,46,82,81,82,81,46,122,123,124,125,126,127,124,125,128,129,0,21,21,21,0,0,0,0,0,23,22,23,22,23,22,23,22,23,22,23,22,24,24,23,22,23,22,23,22,0,23,22,23,22,23,22,87,23,22,0,0,88,0,0,0,88,0,88,88,88,89,91,92,88,89,91,92,88,89,91,92,88,89,91,92,89,91,92,89,91,92,88,89,91,92,88,88,89,91,92,88,89,91,92,22,89,91,92,88,88,89,91,92,88,22,89,91,92,88,89,91,92,88,89,91,92,89,91,92,88,22,89,91,92,22,22,88,22,89,91,92,88,89,91,92,88,89,91,92,88,89,91,92,88,89,91,92,88,130,131,132,0,0,0,0,96,0,96,0,96,0,96,23,23,96,96,100,97,98,99,96,100,97,98,99,96,97,98,99,96,100,97,98,99,100,97,98,99,100,97,98,99,96,100,97,98,99,96,96,100,97,98,99,96,97,98,99,23,100,97,98,99,96,96,100,97,98,99,96,100,97,98,99,96,100,97,98,99,96,100,97,98,99,100,97,98,99,96,97,98,99,96,100,97,98,99,96,23,100,97,98,99,96,100,97,98,99,96,100,97,98,99,96,100,97,98,99,96,133,134,135,136,133,0,0,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,32,33,0,0,32,33,32,33,32,33,32,33,0,0,0,76,76,76,76,0,0,0,0,0,0,0,0,76,76,104,110,111,76,101,102,104,110,111,76,101,102,76,101,102,103,102,101,102,104,104,102,104,110,111,76,101,102,104,110,111,76,101,102,76,102,0,104,110,111,76,101,102,101,102,101,101,101,101,102,102,102,102,32,76,76,101,102,104,110,111,76,101,102,32,104,101,32,104,32,32,32,101,104,110,111,76,101,102,32,104,104,104,110,111,76,101,102,104,110,111,76,101,102,104,101,101,104,110,111,32,104,0,102,112,102,76,101,102,76,104,110,111,76,101,102,104,110,111,76,101,102,104,110,111,76,101,102,102,104,110,111,76,101,102,137,138,139,140,140,36,36,36,36,0,36,0,0,36,36,36,36,36,36,36,36,36,36,36,36,33,33,36,33,33,33,33,36,36,33,33,33,33,33,33,33,36,36,36,36,36,36,141,142,143,144,145,141,142,145,143,144,141],"f":"````````{ce{}{}}000{bc{}}000{bd}{fd}1{cc{}}0{{}b}055{c{{h{e}}}{}{}}0``{c{{j{e}}}{}{}}000{cl{}}088``{nb}{n{{Ab{A`}}}}{nA`}1{{n{Af{Ad}}}{{j{bAh}}}}{{nAj}{{Ab{A`}}}}``{fA`}{f{{j{dAl}}}}00<{{fAn}{{j{dAl}}}}{{f{Af{Ad}}}{{j{bAl}}}}``{ce{}{}}000{B`B`}{{ce}d{}{}}{BbBd}{B`Bd}{BbBf}{B`Bf}{{BbBh}n}{{B`Bh}n}{{BbBh}f}{{B`Bh}f}{{BbBh}Bj}{{B`Bh}Bj}{{BbBh}Bl}{{B`Bh}Bl}{{BbBh}{{Bn{c}}}{C`CbCdCf}}{{B`Bh}{{Bn{c}}}{C`CbCdCf}}{{BbChBh}Cj}{{B`ChBh}Cj}{{BbChClCn}Cj}{{B`CnChCl}Cj}`{{BbCnCh}D`}{{B`CnCh}D`}{{BbCnCh}Db}{{B`CnCh}Db}{{BbBhDd}Df}{{B`BhDd}Df}{{BbCnCh}{{j{dDh}}}}{{B`CnCh}{{j{dDh}}}}{bc{}}000{bd}0{{B`Dj}Dl}{cc{}}0{{}b}0{ce{}{}}0{c{{h{e}}}{}{}}0{BdBb}{{BdDn}B`}{{BdE`}Bb}{BbE`}{BbDn}{B`Dn}{BbB`}8{c{{j{e}}}{}{}}000{cl{}}0::````{cEb{}}{Al{{Ef{Ed}}}}<<{Al{{Ef{Eb}}}}{bc{}}0{AlEh}{bd}{{AlDj}Dl}0{cc{}}{{}b}{ce{}{}}{c{{h{e}}}{}{}}8{cCh{}}==<2````````2222222222`88888888886{Ejd}77{Eld}88{Bjd}77777`666665555544444`````{c{{j{e}}}{}{}}000000000{cl{}}000077777```````{Ej{{j{{Ef{En}}F`}}}}{cEb{}}{F`{{Ef{Ed}}}}::::::::{F`{{Ef{Eb}}}}{FbFb}{{ce}d{}{}}{{}En}{{}Fb}{bc{}}0000000{F`Eh}{bd}0{End}11`{{F`Dj}Dl}0{{FdDj}Dl}{{EnDj}Dl}{{FbDj}Dl}{cc{}}000{FbFf}`{{}b}000{ce{}{}}0000{c{{h{e}}}{}{}}000{EnFf}``{En{{Ef{c}}}{}}```{Ej{{j{dF`}}}}{{EjEn}{{j{dF`}}}}{{EjEnAn}{{j{dF`}}}}`{F`{{Ef{Eb}}}}`7{cCh{}}{c{{j{e}}}{}{}}0000000{cl{}}000`::::```````````{{FhBh}Fh}{cEb{}}{Fj{{Ef{Ed}}}}=========={FhCl}{Fj{{Ef{Eb}}}}{ClCl}{FlFl}{FnFn}{c{{Gb{G`}}}{}}00{{ce}d{}{}}00`{{CjCh}Ej}{{}Fh}{bc{}}000000000{FjEh}{c{{j{Cl}}}Gd}{c{{j{Fl}}}Gd}{c{{j{Fn}}}Gd}{bd}0000{{ClCl}Ff}{{FlFl}Ff}{{FnFn}Ff}{{ClDj}Dl}{{FlDj}Dl}{{FnDj}Dl}{{FjDj}Dl}0{cc{}}0000{{{Af{Ad}}}{{j{ClFj}}}}{Cj{{Gf{Bh}}}}{Fn{{Gj{GhAn}}}}{Cl{{Gj{BhFl}}}}{FnBh}{CjGl}{Cl{{Gf{Bh}}}}{{}b}0000{ce{}{}}0000{c{{h{e}}}{}{}}0000{Cj{{Gf{Gn}}}}`{A`Cl}{{Bh{Gj{GhAn}}}Fn}{{FhBh}Fh}{{FhBhFl}Fh}1{{CjCh{Ef{{Gj{ChAn}}}}}{{j{dH`}}}}{{Clc}jHb}{{Flc}jHb}{{Fnc}jHb}{{c{Hd{{Gf{Ad}}}}}{{j{dHf}}}{}}00{{FhA`}Fh}{Fj{{Ef{Eb}}}}{Cl{{j{{Gf{Ad}}Fj}}}}>>>{cCh{}}{c{{j{e}}}{}{}}000000000{cl{}}0000{ce{}{}}0000```````{{HhGn}{{j{dH`}}}}{cEb{}}{{HhGn}{{j{{Ef{Gh}}H`}}}}{H`{{Ef{Ed}}}}444444{H`{{Ef{Eb}}}}{{HhGn}Ff}{HjHj}{c{{Gb{G`}}}{}}{{ce}d{}{}}{{HhGn}{{j{HlH`}}}}{bc{}}00000{H`Eh}{c{{j{Hj}}}Gd}{bd}00{{HjHj}Ff}{{ce}Ff{}{}}000{{H`Dj}Dl}0{{HjDj}Dl}{cc{}}00{{ce}A`{HnI`}Ib}{Hh{{Gf{Gn}}}}{Hh{{Id{Gh}}}}{{HhGn}{{j{{Id{{If{GhHj}}}}Ih}}}}{Hh{{Gj{GhHj}}}}{{Hjc}dIj}{{}b}00{ce{}{}}00{c{{h{e}}}{}{}}00{H`Ff}{AnHj}`{{HhGnGhHj}{{j{dH`}}}}{{HhGnGh{J`{Il{Gf{In}}}}}{{j{dH`}}}}{{Hjc}jHb}{{c{Hd{{Gf{Ad}}}}}{{j{dHf}}}{}}{H`{{Ef{Eb}}}}8{cCh{}}{c{{j{e}}}{}{}}00000{cl{}}00;;;````````````````{El{{j{dJb}}}}{cEb{}}0{Jd{{Ef{Ed}}}}{Jb{{Ef{Ed}}}}{Df{{j{ElJd}}}}{ce{}{}}000{Jd{{Ef{Eb}}}}{Jb{{Ef{Eb}}}}{El{{j{JfJb}}}}{{ElJh}{{j{dJb}}}}{bc{}}000{JdEh}{JbEh}{bd}0{{JdDj}Dl}0{{JbDj}Dl}0{cc{}}0{{DfJj}{{j{ElJd}}}}{{}b}0=={c{{h{e}}}{}{}}0=<{ElBh}{cCh{}}0{c{{j{e}}}{}{}}000{ElJj}{cl{}}0{ce{}{}}0{{El{Ef{Ch}}{Gf{Ad}}}{{j{dJb}}}}```````````{Bj{{j{dAl}}}}{{Bj{Gf{Ad}}}{{Jl{{j{dAl}}}}}}{{BjCh{Gf{Ad}}}{{Jl{{j{dAl}}}}}}`````4444{bc{}}000{bd}0??{C`{{Gf{{If{EhA`}}}}}}>>>77==`::::88{JnA`}88`````````{cEb{}}{K`{{Ef{Ed}}}}::::::::{{{Kb{c}}}K`{{Kd{Ch}}}}{{{Kf{c}}}K`{{Kd{Ch}}}}{{{Kh{c}}}K`{{Kd{Ch}}}}{K`{{Ef{Eb}}}}{{{Kb{c}}}{{Kb{c}}}Kj}{{{Kf{c}}}{{Kf{c}}}Kj}{{{Kh{c}}}{{Kh{c}}}Kj}{{ce}d{}{}}00========{K`Eh}===={{{Kb{c}}}{{j{eK`}}}{{Kd{Ch}}}{}}{{{Kf{c}}}{{j{eK`}}}{{Kd{Ch}}}{}}{{{Kh{c}}}{{j{eK`}}}{{Kd{Ch}}}{}}{Bl{{j{{Gf{Ad}}K`}}}}{{{Kb{c}}Dj}DlCf}{{{Kf{c}}Dj}DlCf}{{{Kh{c}}Dj}DlCf}{{K`Dj}Dl}0{cc{}}000{Bl{{j{A`K`}}}}{{}b}000{ce{}{}}000{{{Kb{c}}e}K`{{Kd{Ch}}}{}}{{{Kf{c}}e}K`{{Kd{Ch}}}{}}{{{Kh{c}}e}K`{{Kd{Ch}}}{}}{c{{h{e}}}{}{}}000<```{{BlA`A`}{{j{{`{{Kn{}{{Kl{{j{{Gf{Ad}}K`}}}}}}}}K`}}}}{{Bl{If{EhA`}}}{{j{A`K`}}}}{K`{{Ef{Eb}}}}9777{cCh{}}{c{{j{e}}}{}{}}0000000{cl{}}000::::``````````````{{{Bn{c}}c{Gf{Ad}}}{{j{dL`}}}{C`CbCdCf}}{{{Bn{c}}cc{Gf{Ad}}}{{j{dL`}}}{C`CbCdCf}}{cEb{}}{L`{{Ef{Ed}}}}>>>>>>>>>>{{{Lb{c}}}L`{{Kd{Ch}}}}{{{Ld{c}}}L`{{Kd{Ch}}}}{{{Lf{c}}}L`{{Kd{Ch}}}}{L`{{Ef{Eb}}}}{LhLh}{{{Lb{c}}}{{Lb{c}}}Kj}{{{Ld{c}}}{{Ld{c}}}Kj}{{{Lf{c}}}{{Lf{c}}}Kj}{{ce}d{}{}}000{bc{}}000000000{L`Eh}{bd}0000{{{Lb{c}}}{{j{eL`}}}{{Kd{Ch}}}{}}{{{Ld{c}}}{{j{eL`}}}{{Kd{Ch}}}{}}{{{Lf{c}}}{{j{eL`}}}{{Kd{Ch}}}{}}{{{Bn{c}}}{{j{dL`}}}{C`CbCdCf}}{{LhDj}Dl}{{{Lb{c}}Dj}DlCf}{{{Ld{c}}Dj}DlCf}{{{Lf{c}}Dj}DlCf}{{L`Dj}Dl}0{cc{}}0000{{}b}0000{ce{}{}}0000{{Lhc}L`{}}{{{Lb{c}}e}L`{{Kd{Ch}}}{}}{{{Ld{c}}e}L`{{Kd{Ch}}}{}}{{{Lf{c}}e}L`{{Kd{Ch}}}{}}{c{{h{e}}}{}{}}0000```{L`{{Ef{Eb}}}}6666{cCh{}}{{{Bn{c}}A`}{{j{dL`}}}{C`CbCdCf}}{c{{j{e}}}{}{}}000000000{cl{}}0000:::::```````::::{bc{}}000{bd}0>>==<<77``333322<<```````````````{cEb{}}{Ih{{Ef{Ed}}}}>>>>>>>>>>>>{Ih{{Ef{Eb}}}}{LjLj}{LlLl}{Ln{{Gb{G`}}}}{c{{Gb{G`}}}{}}{{ce}d{}{}}0{{M`EhEh}Ff}{{M`Eh}Ff}`;;;;;;;;;;;;{IhEh}{c{{j{Ll}}}Gd}{{{Af{Ad}}}{{j{cHf}}}Mb}======{{LjLj}Ff}{{LlLl}Ff}{{ce}Ff{}{}}0000000{D`{{j{MdDh}}}}{{IhDj}Dl}0{{LjDj}Dl}{{LlDj}Dl}{cc{}}00000{{D`EhEh}{{Ef{Ll}}}}{{M`EhEh}{{Ef{Ll}}}}{{ce}A`{HnI`}Ib}{{D`Eh}{{Gj{ChLl}}}}{{M`Eh}{{Gj{ChLl}}}}{{D`Eh{Ef{Ch}}}Mf}{D`Ch}{D`{{Gj{Ch{Gj{ChLl}}}}}}{{Ljc}dIj}{{}b}00000{{D`e}{{j{cIh}}}{}{{Mj{M`}{{Mh{{j{cIh}}}}}}}}{{M`ChChCh{Gb{G`}}}d}{{M`ChCh}{{j{dIh}}}}{ce{}{}}00000{c{{h{e}}}{}{}}00000{M`Ff}``{{{Gj{Ch{Gj{ChLl}}}}{Gj{ChLl}}{Gf{Ml}}{Gf{Mn}}}M`}{{Ch{Ef{Ch}}Ch}Ml}{{ChCh}Mn}8{{M`Ch}d}{G`{{j{{Gf{Ad}}Hf}}}}{{Llc}jHb}{{N`{Hd{{Gf{Ad}}}}}{{j{dHf}}}}{{c{Hd{{Gf{Ad}}}}}{{j{dHf}}}{}}{Ih{{Ef{Eb}}}};;{cCh{}}{c{{j{e}}}{}{}}00000000000{cl{}}00000`>>>>>>`````````````{cEb{}}{Dh{{Ef{Ed}}}}{ce{}{}}0{Dh{{Ef{Eb}}}}{bc{}}0{DhEh}{bd}{{DhDj}Dl}0{cc{}}{{Dbc}{{j{{Ef{{If{eMf}}}}Dh}}}{NbMb}{NbMb}}{{Db{Gf{c}}}{{j{{Gf{{Ef{{If{eMf}}}}}}Dh}}}{NbMb}{NbMb}}{{}b}{{DbceAn}{{j{MfDh}}}{NbMb}{NbMb}}{{Db{Gf{{If{ce}}}}An}{{j{{Gf{Mf}}Dh}}}{NbMb}{NbMb}}{{DbceMfAn}{{j{MfDh}}}{NbMb}{NbMb}}{{Db{Gf{{If{ceMf}}}}An}{{j{{Gf{Mf}}Dh}}}{NbMb}{NbMb}}={c{{h{e}}}{}{}}{{DbMd}{{`{{Kn{}{{Kl{{j{{If{ceMf}}Dh}}}}}}}}}{NbMbNd}{NbMbNd}}{{DbMdAn}{{`{{Kn{}{{Kl{{j{{If{ceMfAn}}Dh}}}}}}}}}{NbMbNd}{NbMbNd}}{{DbMd}{{`{{Kn{}{{Kl{{j{{If{cMf}}Dh}}}}}}}}}{NbMbNd}}{{DbcAn}{{j{dDh}}}{NbMb}}{{Db{Gf{c}}An}{{j{dDh}}}{NbMb}}{{DbcMfAn}{{j{dDh}}}{NbMb}}{{Db{Gf{{If{cMf}}}}An}{{j{dDh}}}{NbMb}}{Dh{{Ef{Eb}}}}{cCh{}}{c{{j{e}}}{}{}}0{cl{}}{ce{}{}}```````````","D":"AOn","p":[[1,"usize"],[1,"unit"],[5,"ByteWriter",6,46],[5,"Request",1116],[6,"Result",1117],[5,"TypeId",1118],[5,"ByteReader",6,38],[1,"u64"],[8,"Result",1119],[1,"u8"],[1,"slice"],[5,"Error",1119],[6,"SeekFrom",1120],[6,"Error",120],[1,"i64"],[5,"ClientFactoryAsync",54],[5,"ClientFactory",54],[5,"ClientConfig",1121],[10,"ControllerClient",1122],[5,"ScopedStream",1123],[5,"EventWriter",148,613],[5,"IndexReader",617,653],[5,"IndexWriter",617,754],[10,"Fields",617],[10,"PartialOrd",1124],[10,"PartialEq",1124],[10,"Debug",1125],[5,"String",1126],[5,"ReaderGroup",148,323],[5,"ReaderGroupConfig",323],[5,"Scope",1123],[5,"Synchronizer",876,906],[5,"Table",876,1063],[5,"WriterId",1123],[5,"TransactionalEventWriter",148,541],[6,"TableError",1063],[5,"Formatter",1125],[8,"Result",1125],[5,"Handle",1127],[5,"Runtime",1128],[10,"Error",1129],[5,"Backtrace",1130],[6,"Option",1131],[1,"str"],[5,"EventReader",148,229],[5,"Transaction",148,541],[5,"SegmentSlice",229],[6,"EventReaderError",229],[5,"SliceMetadata",229],[5,"Event",229],[1,"bool"],[5,"ReaderGroupConfigBuilder",323],[6,"SerdeError",323],[6,"StreamCutVersioned",323],[5,"StreamCutV1",323],[10,"ValueData",906],[5,"Box",1132],[10,"Deserializer",1133],[5,"Vec",1134],[5,"ScopedSegment",1123],[5,"HashMap",1135],[5,"StreamCut",1123],[5,"Reader",1123],[6,"ReaderGroupStateError",457],[10,"Serializer",1136],[5,"Serializer",1137],[5,"Error",1138],[5,"ReaderGroupState",457],[5,"Offset",457],[1,"isize"],[10,"Hash",1139],[10,"Sized",1140],[10,"BuildHasher",1139],[5,"HashSet",1141],[1,"tuple"],[6,"SynchronizerError",906],[10,"Hasher",1139],[5,"SegmentWithRange",1123],[5,"Segment",1123],[5,"HashMap",1142],[6,"TransactionError",541],[6,"TransactionalEventWriterError",541],[6,"TransactionStatus",1123],[5,"Timestamp",1123],[5,"TxId",1123],[5,"Receiver",1143],[10,"Value",617],[6,"IndexReaderError",653],[5,"FieldNotFound",653],[10,"Into",1144],[5,"InvalidOffset",653],[5,"Internal",653],[10,"Clone",1145],[17,"Item"],[10,"Stream",1146],[6,"IndexWriterError",754],[5,"InvalidFields",754],[5,"InvalidCondition",754],[5,"Internal",754],[5,"InvalidData",754],[5,"Key",906],[5,"Value",906],[10,"ValueClone",906],[5,"Update",906],[10,"DeserializeOwned",1133],[1,"i32"],[8,"Version",1063],[17,"Output"],[10,"FnMut",1147],[5,"Insert",906],[5,"Remove",906],[10,"ValueSerialize",906],[10,"Serialize",1136],[10,"Unpin",1140],[15,"ConditionalCheckFailure",145],[15,"InternalFailure",145],[15,"InvalidInput",145],[15,"StateError",322],[15,"Cbor",455],[15,"ReaderAlreadyOfflineError",537],[15,"SyncError",537],[15,"TxnSegmentWriterError",603],[15,"TxnClosed",603],[15,"TxnCommitError",603],[15,"TxnAbortError",603],[15,"TxnStreamWriterError",603],[15,"TxnControllerError",603],[15,"PingerError",611],[15,"TxnStreamControllerError",611],[15,"FieldNotFound",751],[15,"InvalidOffset",751],[15,"Internal",751],[15,"InvalidData",871],[15,"InvalidFields",871],[15,"InvalidCondition",871],[15,"Internal",871],[15,"SyncUpdateError",1058],[15,"SyncTombstoneError",1058],[15,"SyncPreconditionError",1058],[15,"SyncTableError",1058],[15,"ConnectionError",1105],[15,"KeyDoesNotExist",1105],[15,"IncorrectKeyVersion",1105],[15,"OperationError",1105],[15,"TableDoesNotExist",1105]],"r":[[6,38],[7,46],[148,229],[149,613],[150,323],[151,541],[152,541],[618,653],[619,754],[876,906],[877,1063]],"b":[[133,"impl-Display-for-Error"],[134,"impl-Debug-for-Error"],[266,"impl-Display-for-EventReaderError"],[267,"impl-Debug-for-EventReaderError"],[385,"impl-Debug-for-SerdeError"],[386,"impl-Display-for-SerdeError"],[494,"impl-Display-for-ReaderGroupStateError"],[495,"impl-Debug-for-ReaderGroupStateError"],[575,"impl-Debug-for-TransactionalEventWriterError"],[576,"impl-Display-for-TransactionalEventWriterError"],[577,"impl-Display-for-TransactionError"],[578,"impl-Debug-for-TransactionError"],[701,"impl-Display-for-IndexReaderError"],[702,"impl-Debug-for-IndexReaderError"],[815,"impl-Debug-for-IndexWriterError"],[816,"impl-Display-for-IndexWriterError"],[977,"impl-Display-for-SynchronizerError"],[978,"impl-Debug-for-SynchronizerError"],[1080,"impl-Display-for-TableError"],[1081,"impl-Debug-for-TableError"]],"c":"OjAAAAAAAAA=","e":"OzAAAAEAAF0DXwADAAAACQAKABYAAQAaAAwALgAAADgAFQBRAAYAWgAGAGMAAQBnACAAiQAAAIsACQCaABUAsQAFALwABQDHAB4A6AAAAOoAAQDtACIAFQEEAB4BCwAtARcASAEAAEsBAgBPAQsAXAEKAGgBGwCQAQQAmgEEAKABAACiAQAApwEFAK4BAACwARsAzQEBANABAADSAQsA3wESAPUBAAD6AQMAAQIDAAkCFQAgAgAAIgIHACsCAwAwAgUAOAILAEcCAQBLAgMAUAIFAFcCAwBcAgkAagIAAG0CCwB7AgMAgQIMAI8CAACRAgAAkwIAAJUCCgCjAhMAuwIEAMUCAwDNAgYA1QICANoCAADcAhcA9QIAAPcCAAD5AgAA+wIAAP0CAAAAAwsADwMYACwDBQA3AwQAQQMRAFQDGABvAwkAewMBAH8DCwCOAwMAkwMBAJoDFACxAw4AwQMPANIDAwDeAwAA5AMGAPQDCwADBCgALQQNAD4EAABEBAAATAQQAA=="}]\
]'));
if (typeof exports !== 'undefined') exports.searchIndex = searchIndex;
else if (window.initSearch) window.initSearch(searchIndex);
