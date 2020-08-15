use rustyline::error::ReadlineError;
use rustyline::Editor;
use shlex;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use trigram_rs;

trait Cmd {
    fn run(&mut self, cmd: &String, args: &Vec<String>) -> Result<(), String>;
}

struct Indexer {
    idx: Option<trigram_rs::Index>,
    docs: Option<Vec<String>>,
    ids: Option<Vec<trigram_rs::DocID>>,
}

impl Cmd for Indexer {
    fn run(&mut self, cmd: &String, args: &Vec<String>) -> Result<(), String> {
        match cmd.as_str() {
            "index" => self.run_index(args),
            "search" => self.run_search(args),
            "print" => self.run_print(args),
            "brute" => self.run_brute(args),
            _ => Err("unknown command".to_string()),
        }
    }
}

impl Indexer {
    fn run_index(&mut self, args: &Vec<String>) -> Result<(), String> {
        let mut docs = Vec::<String>::new();

        let file = match File::open(Path::new(&args[0])) {
            Ok(f) => f,
            Err(err) => return Err(format!("{}", err)),
        };

        let reader = BufReader::new(file);

        for l in reader.lines() {
            match l {
                Ok(l) => docs.push(l),
                Err(err) => return Err(format!("unable to read line: {}", err)),
            }
        }

        let strdocs: Vec<&str> = docs.iter().map(AsRef::as_ref).collect();

        let t0 = Instant::now();
        let idx = trigram_rs::Index::new_with_documents(strdocs);

        println!(
            "indexed {} documents in {}ms",
            docs.len(),
            t0.elapsed().as_millis()
        );

        self.idx = Some(idx);
        self.docs = Some(docs);
        self.ids = None;

        Ok(())
    }

    fn run_search(&mut self, args: &Vec<String>) -> Result<(), String> {
        if self.idx.is_none() {
            return Err("no index loaded".to_string());
        }

        let mut trigrams = Vec::<trigram_rs::T>::new();
        let mut ts = Vec::<trigram_rs::T>::new();
        for q in args.iter() {
            trigram_rs::extract_all_trigrams(q, &mut ts);
            trigrams.extend(&ts);
            ts.clear()
        }

        for _ in 0..30 {
            let t0 = Instant::now();
            let mut found = 0usize;
            for _ in 0..100 {
                let ids = self.idx.as_ref().unwrap().query_trigrams(&trigrams);
                found = ids.len();
                self.ids = Some(ids);
            }
            println!("found {} hits in {}ms", found, t0.elapsed().as_millis());
        }

        Ok(())
    }

    fn run_print(&self, _args: &Vec<String>) -> Result<(), String> {
        if self.ids.is_none() {
            return Err("no search results".to_string());
        }

        let docs = self.docs.as_ref().unwrap();
        let ids = self.ids.as_ref().unwrap();

        for id in ids {
            println!("{}", docs[id.as_usize()]);
        }

        Ok(())
    }

    fn run_brute(&mut self, args: &Vec<String>) -> Result<(), String> {
        let docs = match &self.docs {
            None => return Err("no index loaded".to_string()),
            Some(docs) => docs,
        };

        if args.len() == 0 {
            return Err("missing query".to_string());
        }

        let mut ids = Vec::<trigram_rs::DocID>::new();
        let t0 = Instant::now();

        let patterns = args;

        'next_document: for (i, s) in docs.iter().enumerate() {
            for pat in patterns {
                if !s.contains(pat) {
                    continue 'next_document;
                }
            }

            ids.push(trigram_rs::DocID::from_i32(i as i32));
        }

        println!(
            "found {} documents in {}ms",
            ids.len(),
            t0.elapsed().as_millis()
        );

        self.ids = Some(ids);

        Ok(())
    }
}

fn run(prompt: &str, mut commands: impl Cmd) {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let words = match shlex::split(&line) {
                    None => {
                        println!("syntax error");
                        continue;
                    }
                    Some(words) => words,
                };

                let (cmd, argv) = match words.split_first() {
                    None => continue,
                    Some((first, rest)) => (first, rest),
                };

                match commands.run(&cmd, &argv.to_vec()) {
                    Ok(()) => {}
                    Err(err) => println!("error: {}", err),
                }
            }

            Err(ReadlineError::Interrupted) => {
                println!("bye");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("bye");
                break;
            }
            Err(err) => {
                println!("error: {:?}", err);
                break;
            }
        }
    }
}

fn main() {
    let commands = Indexer {
        idx: None,
        docs: None,
        ids: None,
    };

    run("> ", commands);
}
