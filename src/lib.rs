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

/// Index is a trigram index
pub struct Index(HashMap<T, Option<Vec<DocID>>>);

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
const TAllDocIDs: T = T(0xFFFFFFFF);

// Extract returns a list of all the unique trigrams in s
pub fn extract_trigrams(s: &str) -> Vec<T> {
    let mut trigrams: Vec<T> = Vec::new();

    let bytes = s.as_bytes();

    if s.len() < 3 {
        return trigrams;
    }

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

// NewIndex returns an index for the strings in docs
fn new_index(docs: Vec<&str>) -> Index {
    let mut idx = HashMap::<T, Option<Vec<DocID>>>::new();
    let mut all_doc_ids = Vec::<DocID>::new();
    let mut trigrams = Vec::<T>::new();

    for (id, &d) in docs.iter().enumerate() {
        extract_all_trigrams(d, &mut trigrams);
        let docid = DocID(id as i32);

        all_doc_ids.push(docid);

        for t in trigrams.iter() {
            match idx.get_mut(&t) {
                None => {
                    idx.insert(t.clone(), Some(vec![docid]));
                }
                Some(oidxt) => match oidxt.as_ref().unwrap().last() {
                    None => {
                        let idxt = oidxt.as_mut().unwrap();
                        idxt.push(docid);
                    }
                    Some(did) => {
                        if did != &docid {
                            let idxt = oidxt.as_mut().unwrap();
                            idxt.push(docid);
                        }
                    }
                },
            }
        }
        trigrams.clear();
    }

    idx.insert(TAllDocIDs, Some(all_doc_ids));

    return Index(idx);
}

impl Index {
    pub fn query(&self, s: &str) -> Vec<DocID> {
        let ts = extract_trigrams(s);
        return self.query_trigrams(ts);
    }

    fn copy_all_docs(&self) -> Vec<DocID> {
        let all = self.0.get(&TAllDocIDs).unwrap().as_ref().unwrap();
        all.clone()
    }

    pub fn query_trigrams(&self, trigrams: Vec<T>) -> Vec<DocID> {
        if trigrams.len() == 0 {
            return self.copy_all_docs();
        }

        let mut freqs = Vec::<TermFrequency>::new();
        for t in trigrams.iter() {
            let d = match self.0.get(t) {
                None => return Vec::<DocID>::new(),
                Some(d) => d,
            };
            freqs.push(TermFrequency {
                t: *t,
                freq: match d {
                    None => 0,
                    Some(d) => d.len(),
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
                None => return Vec::<DocID>::new(),
                Some(d) => return self.filter(d, rest.to_vec()),
            },
        };
    }

    pub fn prune(&mut self, percent: f64) -> usize {
        let max_documents =
            (percent * self.0.get(&TAllDocIDs).unwrap().as_ref().unwrap().len() as f64) as usize;

        let mut pruned = 0usize;

        // Update all values
        for (_, v) in self.0.iter_mut() {
            if v.is_some() && v.as_ref().unwrap().len() > max_documents {
                pruned += 1;
                *v = None;
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
                None => {
                    // the trigram was removed via Prune()
                    continue;
                }
                Some(d) => d,
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

        let idx = new_index(docs);

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
    }
}
