use std::collections::HashMap;

/// T is a trigram
#[derive(Eq, Hash, Clone, Copy, PartialEq)]
pub struct T(u32);

use std::fmt;

impl fmt::Display for T {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "T({}{}{})",
            (self.0 >> 16) as u8 as char,
            (self.0 >> 8) as u8 as char,
            self.0 as u8 as char,
        )
    }
}

impl fmt::Debug for T {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "T({}{}{})",
            (self.0 >> 16) as u8 as char,
            (self.0 >> 8) as u8 as char,
            self.0 as u8 as char,
        )
    }
}

/// DocID is a document ID
#[derive(Debug, Eq, Copy, Clone, PartialEq, PartialOrd)]
pub struct DocID(i32);

enum Posting {
    Pruned,
    List(Vec<DocID>),
}

/// Index is a trigram index
pub struct Index(HashMap<T, Posting>);

#[derive(Debug)]
struct TermFrequency {
    t: T,
    freq: usize,
}

use std::cmp::{Ord, Ordering, PartialOrd};

impl PartialEq for TermFrequency {
    fn eq(&self, other: &Self) -> bool {
        self.freq.eq(&other.freq)
    }
}

impl PartialOrd for TermFrequency {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.freq.partial_cmp(&other.freq)
    }
}

impl Ord for TermFrequency {
    fn cmp(&self, other: &Self) -> Ordering {
        self.freq.cmp(&other.freq)
    }
}

impl Eq for TermFrequency {}

// a special (and invalid) trigram that holds all the document IDs
const ALL_DOC_IDS: T = T(0xFFFFFFFF);

// Extract returns a list of all the unique trigrams in s
pub fn extract_trigrams(s: &str) -> Vec<T> {
    let mut trigrams: Vec<T> = Vec::new();

    if s.len() < 3 {
        return trigrams;
    }
    let bytes = s.as_bytes();

    for i in 0..=bytes.len() - 3 {
        let t: T = T((bytes[i] as u32) << 16 | (bytes[i + 1] as u32) << 8 | bytes[i + 2] as u32);
        trigrams = append_if_unique(trigrams, t);
    }

    return trigrams;
}

fn append_if_unique(mut trigrams: Vec<T>, t: T) -> Vec<T> {
    if !trigrams.contains(&t) {
        trigrams.push(t)
    }
    return trigrams;
}

// Extract All returns a list of all the unique trigrams in s
pub fn extract_all_trigrams(s: &str, trigrams: &mut Vec<T>) {
    let bytes = s.as_bytes();

    for i in 0..=bytes.len() - 3 {
        let t: T = T((bytes[i] as u32) << 16 | (bytes[i + 1] as u32) << 8 | bytes[i + 2] as u32);
        trigrams.push(t);
    }
}

impl Index {
    // NewIndex returns an index for the strings in docs
    pub fn new_with_documents(docs: Vec<&str>) -> Index {
        let mut idx = HashMap::<T, Posting>::new();
        let mut all_doc_ids = Vec::<DocID>::new();
        let mut trigrams = Vec::<T>::new();

        for (id, &d) in docs.iter().enumerate() {
            extract_all_trigrams(d, &mut trigrams);
            let docid = DocID(id as i32);

            all_doc_ids.push(docid);

            for t in trigrams.iter() {
                match idx.get_mut(&t) {
                    None => {
                        idx.insert(t.clone(), Posting::List(vec![docid]));
                    }
                    Some(oidxt) => match oidxt {
                        Posting::Pruned => panic!("pruned list found during index construction"),
                        Posting::List(idxt) => match idxt.last() {
                            None => idxt.push(docid),
                            Some(did) => {
                                if did != &docid {
                                    idxt.push(docid);
                                }
                            }
                        },
                    },
                }
            }
            trigrams.clear();
        }

        idx.insert(ALL_DOC_IDS, Posting::List(all_doc_ids));

        return Index(idx);
    }

    pub fn add(&mut self, s: &str) -> DocID {
        let id = DocID(self.get_all_docs().len() as i32);
        self.insert(s, id);
        return id;
    }

    pub fn add_trigrams(&mut self, ts: &Vec<T>) -> DocID {
        let id = DocID(self.get_all_docs().len() as i32);
        self.insert_trigrams(ts, id);
        return id;
    }

    pub fn insert(&mut self, s: &str, id: DocID) {
        let mut ts = Vec::<T>::new();
        extract_all_trigrams(s, &mut ts);
        self.insert_trigrams(&ts, id);
    }

    pub fn insert_trigrams(&mut self, ts: &Vec<T>, id: DocID) {
        for t in ts.iter() {
            match self.0.get_mut(&t) {
                None => {
                    self.0.insert(t.clone(), Posting::List(vec![id]));
                }
                Some(oidxt) => match oidxt {
                    Posting::Pruned => { /* trigram post list has been pruned; it must be kept empty */
                    }
                    Posting::List(idxt) => match idxt.last() {
                        None => idxt.push(id),
                        Some(did) => {
                            if did != &id {
                                idxt.push(id);
                            }
                        }
                    },
                },
            }
        }

        let all = self.get_all_docs_mut();
        all.push(id);
    }

    pub fn query(&self, s: &str) -> Vec<DocID> {
        let ts = extract_trigrams(s);
        return self.query_trigrams(ts);
    }

    fn get_all_docs(&self) -> &Vec<DocID> {
        let all = match self.0.get(&ALL_DOC_IDS).unwrap() {
            Posting::Pruned => panic!("all docs pruned"),
            Posting::List(l) => l,
        };
        all
    }

    fn get_all_docs_mut(&mut self) -> &mut Vec<DocID> {
        let all = match self.0.get_mut(&ALL_DOC_IDS).unwrap() {
            Posting::Pruned => panic!("all docs pruned"),
            Posting::List(l) => l,
        };
        all
    }

    fn copy_all_docs(&self) -> Vec<DocID> {
        self.get_all_docs().clone()
    }

    pub fn query_trigrams(&self, trigrams: Vec<T>) -> Vec<DocID> {
        if trigrams.len() == 0 {
            return self.copy_all_docs();
        }

        let mut freqs = Vec::<TermFrequency>::with_capacity(trigrams.len());
        for t in trigrams.iter() {
            let d = match self.0.get(t) {
                None => return Vec::<DocID>::new(),
                Some(d) => d,
            };
            freqs.push(TermFrequency {
                t: *t,
                freq: match d {
                    Posting::Pruned => 0,
                    Posting::List(d) => d.len(),
                },
            });
        }

        freqs.sort_unstable();

        let mut nonzero = 0usize;
        while nonzero < freqs.len() && freqs[nonzero].freq == 0 {
            nonzero += 1;
        }

        // all the trigrams have been pruned; return all docs
        if nonzero == freqs.len() {
            return self.copy_all_docs();
        }
        let mut ts = Vec::<T>::new();
        ts.reserve(freqs.len() - nonzero);

        // skip over pruned trigrams
        for tf in freqs[nonzero..freqs.len()].iter() {
            ts.push(tf.t);
        }

        let (first, rest) = ts.split_first().unwrap();

        let docs = self.0.get(first);

        match docs {
            None => return Vec::<DocID>::new(),
            Some(docs) => match docs {
                Posting::Pruned => return Vec::<DocID>::new(),
                Posting::List(d) => return self.filter(d, rest.to_vec()),
            },
        };
    }

    pub fn prune(&mut self, percent: f64) -> usize {
        let max_documents = (percent * (self.get_all_docs().len() as f64)) as usize;

        let mut pruned = 0usize;

        // Update all values
        for (_, v) in self.0.iter_mut() {
            match v {
                Posting::Pruned => continue,
                Posting::List(l) => {
                    if l.len() > max_documents {
                        pruned += 1;
                        *v = Posting::Pruned;
                    }
                }
            }
        }

        pruned
    }

    // Filter removes documents that don't contain the specified trigrams
    pub fn filter(&self, docs: &Vec<DocID>, ts: Vec<T>) -> Vec<DocID> {
        // no provided filter trigrams
        if ts.len() == 0 {
            return docs.clone();
        }

        // interesting implementation detail:
        // we don't want to repurpose/alter docs since it's typically
        // a live postings list, hence allocating a result slice
        // however, upon subsequent loop runs we do repurpose the input
        // as the output, because at that point its safe for reuse

        let mut result = Vec::<DocID>::new();
        result.resize(docs.len(), DocID(0));

        let mut first = true;

        for t in ts.iter() {
            let d = match self.0.get(t) {
                None => return Vec::<DocID>::new(),
                Some(d) => d,
            };

            let d = match d {
                Posting::Pruned => continue,
                Posting::List(l) => l,
            };

            if first {
                intersect3(&mut result, &docs, d);
                first = false;
            } else {
                intersect2(&mut result, d);
            }
        }

        return result;
    }
}

// intersect intersects the input slices and puts the output in result slice
// note that result may be backed by the same array as a or b, since
// we only add docs that also exist in both inputs, it's guaranteed that we
// never overwrite/clobber the input, as long as result's start and len are proper
fn intersect3(result: &mut Vec<DocID>, a: &Vec<DocID>, b: &Vec<DocID>) {
    let mut aidx = 0usize;
    let mut bidx = 0usize;
    let mut ridx: usize = 0usize;

    'scan: while aidx < a.len() && bidx < b.len() {
        if a[aidx] == b[bidx] {
            result[ridx] = a[aidx];
            ridx += 1;
            aidx += 1;
            bidx += 1;
            if aidx == a.len() || bidx == b.len() {
                break 'scan;
            }
        }

        while a[aidx] < b[bidx] {
            aidx += 1;
            if aidx == a.len() {
                break 'scan;
            }
        }

        while a[aidx] > b[bidx] {
            bidx += 1;
            if bidx == b.len() {
                break 'scan;
            }
        }
    }
    result.truncate(ridx);
}

fn intersect2(a: &mut Vec<DocID>, b: &Vec<DocID>) {
    let mut aidx = 0usize;
    let mut bidx = 0usize;
    let mut ridx: usize = 0usize;

    'scan: while aidx < a.len() && bidx < b.len() {
        if a[aidx] == b[bidx] {
            a[ridx] = a[aidx];
            ridx += 1;
            aidx += 1;
            bidx += 1;
            if aidx == a.len() || bidx == b.len() {
                break 'scan;
            }
        }

        while a[aidx] < b[bidx] {
            aidx += 1;
            if aidx == a.len() {
                break 'scan;
            }
        }

        while a[aidx] > b[bidx] {
            bidx += 1;
            if bidx == b.len() {
                break 'scan;
            }
        }
    }

    a.truncate(ridx);
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_query() {
        let docs = vec!["foo", "foobar", "foobfoo", "quxzoot", "zotzot", "azotfoba"];

        let mut idx = Index::new_with_documents(docs);

        macro_rules! test_query {
            ($q:expr, $want:expr) => {{
                let got = idx.query($q);
                assert_eq!(got, $want);
            }};
        }

        test_query!(
            "",
            vec![DocID(0), DocID(1), DocID(2), DocID(3), DocID(4), DocID(5)]
        );

        test_query!("foo", vec![DocID(0), DocID(1), DocID(2)]);
        test_query!("foob", vec![DocID(1), DocID(2)]);
        test_query!("zot", vec![DocID(4), DocID(5)]);
        test_query!("oba", vec![DocID(1), DocID(5)]);

        idx.add("quxlzot"); // 6
        idx.add("zottlequx"); // 7
        idx.add("bazlefob"); // 8

        test_query!("zottle", vec![DocID(7)]);
    }
}
