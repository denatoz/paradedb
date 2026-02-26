// Copyright (c) 2023-2026 ParadeDB, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

/*
 *
 * IMPORTANT NOTICE:
 * This file has been copied from Quickwit, an open source project, and is subject to the terms
 * and conditions of the GNU Affero General Public License (AGPL) version 3.0.
 * Please review the full licensing details at <http://www.gnu.org/licenses/>.
 * By using this file, you agree to comply with the AGPL v3.0 terms.
 *
 */
use lindera::dictionary::{load_dictionary, load_user_dictionary_from_csv};
use lindera::mode::Mode;
use lindera::token::Token as LinderaToken;
use lindera::tokenizer::Tokenizer as LinderaTokenizer;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

// Tokenizers with keep_whitespace=true to maintain backward compatibility
// with previous ParadeDB behavior. Lindera 1.4.0+ defaults to false (MeCab-compatible),
// but we preserve whitespace tokens for existing indexes.
static CMN_TOKENIZER: Lazy<Arc<LinderaTokenizer>> = Lazy::new(|| {
    let dictionary = load_dictionary("embedded://cc-cedict")
        .expect("Lindera `cc-cedict` dictionary must be present");
    Arc::new(LinderaTokenizer::new(
        lindera::segmenter::Segmenter::new(Mode::Normal, dictionary, None).keep_whitespace(true),
    ))
});

static JPN_TOKENIZER: Lazy<Arc<LinderaTokenizer>> = Lazy::new(|| {
    let dictionary =
        load_dictionary("embedded://ipadic").expect("Lindera `ipadic` dictionary must be present");
    Arc::new(LinderaTokenizer::new(
        lindera::segmenter::Segmenter::new(Mode::Normal, dictionary, None).keep_whitespace(true),
    ))
});

static KOR_TOKENIZER: Lazy<Arc<LinderaTokenizer>> = Lazy::new(|| {
    let dictionary =
        load_dictionary("embedded://ko-dic").expect("Lindera `ko-dic` dictionary must be present");
    Arc::new(LinderaTokenizer::new(
        lindera::segmenter::Segmenter::new(Mode::Normal, dictionary, None).keep_whitespace(true),
    ))
});

// Cache for tokenizers with user dictionaries, keyed by (language, user_dict_path)
static USER_DICT_TOKENIZER_CACHE: Lazy<Mutex<HashMap<String, Arc<LinderaTokenizer>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn dict_uri_for_language(language: &str) -> &'static str {
    match language {
        "chinese" => "embedded://cc-cedict",
        "japanese" => "embedded://ipadic",
        "korean" => "embedded://ko-dic",
        _ => panic!("unsupported lindera language: {language}"),
    }
}

/// Create or retrieve a cached Lindera tokenizer with a user dictionary.
pub fn get_tokenizer_with_user_dict(
    language: &str,
    user_dict_path: &str,
) -> Arc<LinderaTokenizer> {
    let cache_key = format!("{language}:{user_dict_path}");
    let mut cache = USER_DICT_TOKENIZER_CACHE.lock().unwrap();
    if let Some(tok) = cache.get(&cache_key) {
        return tok.clone();
    }

    let dictionary = load_dictionary(dict_uri_for_language(language))
        .unwrap_or_else(|e| panic!("failed to load {language} dictionary: {e}"));

    let user_dict =
        load_user_dictionary_from_csv(&dictionary.metadata, Path::new(user_dict_path))
            .unwrap_or_else(|e| {
                panic!("failed to load user dictionary from {user_dict_path}: {e}")
            });

    let tokenizer = Arc::new(LinderaTokenizer::new(
        lindera::segmenter::Segmenter::new(Mode::Normal, dictionary, Some(user_dict))
            .keep_whitespace(true),
    ));

    cache.insert(cache_key, tokenizer.clone());
    tokenizer
}

/// A Lindera tokenizer wrapper that uses a custom (user dictionary-loaded) tokenizer instance.
#[derive(Clone)]
pub struct LinderaTokenizerWithDict {
    tokenizer: Arc<LinderaTokenizer>,
    token: Token,
}

impl LinderaTokenizerWithDict {
    pub fn new(tokenizer: Arc<LinderaTokenizer>) -> Self {
        Self {
            tokenizer,
            token: Token::default(),
        }
    }
}

impl Tokenizer for LinderaTokenizerWithDict {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }

        let lindera_token_stream = LinderaTokenStream {
            tokens: self
                .tokenizer
                .tokenize(text)
                .expect("Lindera tokenizer with user dictionary failed"),
            token: &mut self.token,
        };

        MultiLanguageTokenStream::Lindera(lindera_token_stream)
    }
}

#[derive(Clone, Default)]
pub struct LinderaChineseTokenizer {
    token: Token,
}

#[derive(Clone, Default)]
pub struct LinderaJapaneseTokenizer {
    token: Token,
}

#[derive(Clone, Default)]
pub struct LinderaKoreanTokenizer {
    token: Token,
}

impl Tokenizer for LinderaChineseTokenizer {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }

        let lindera_token_stream = LinderaTokenStream {
            tokens: CMN_TOKENIZER
                .tokenize(text)
                .expect("Lindera Chinese tokenizer failed"),
            token: &mut self.token,
        };

        MultiLanguageTokenStream::Lindera(lindera_token_stream)
    }
}

impl Tokenizer for LinderaJapaneseTokenizer {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }

        let lindera_token_stream = LinderaTokenStream {
            tokens: JPN_TOKENIZER
                .tokenize(text)
                .expect("Lindera Japanese tokenizer failed"),
            token: &mut self.token,
        };

        MultiLanguageTokenStream::Lindera(lindera_token_stream)
    }
}

impl Tokenizer for LinderaKoreanTokenizer {
    type TokenStream<'a> = MultiLanguageTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        if text.trim().is_empty() {
            return MultiLanguageTokenStream::Empty;
        }

        let lindera_token_stream = LinderaTokenStream {
            tokens: KOR_TOKENIZER
                .tokenize(text)
                .expect("Lindera Korean tokenizer failed"),
            token: &mut self.token,
        };

        MultiLanguageTokenStream::Lindera(lindera_token_stream)
    }
}

pub enum MultiLanguageTokenStream<'a> {
    Empty,
    Lindera(LinderaTokenStream<'a>),
}

pub struct LinderaTokenStream<'a> {
    pub tokens: Vec<LinderaToken<'a>>,
    pub token: &'a mut Token,
}

impl TokenStream for MultiLanguageTokenStream<'_> {
    fn advance(&mut self) -> bool {
        match self {
            MultiLanguageTokenStream::Empty => false,
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.advance(),
        }
    }

    fn token(&self) -> &Token {
        match self {
            MultiLanguageTokenStream::Empty => {
                panic!("Cannot call token() on an empty token stream.")
            }
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.token(),
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        match self {
            MultiLanguageTokenStream::Empty => {
                panic!("Cannot call token_mut() on an empty token stream.")
            }
            MultiLanguageTokenStream::Lindera(tokenizer) => tokenizer.token_mut(),
        }
    }
}

impl TokenStream for LinderaTokenStream<'_> {
    fn advance(&mut self) -> bool {
        if self.tokens.is_empty() {
            return false;
        }
        let token = self.tokens.remove(0);
        self.token.text = token.surface.to_string();
        self.token.offset_from = token.byte_start;
        self.token.offset_to = token.byte_end;
        self.token.position = token.position;
        self.token.position_length = token.position_length;

        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

    fn test_helper<T: Tokenizer>(tokenizer: &mut T, text: &str) -> Vec<Token> {
        let mut token_stream = tokenizer.token_stream(text);
        let mut tokens: Vec<Token> = vec![];
        while token_stream.advance() {
            tokens.push(token_stream.token().clone());
        }
        tokens
    }

    #[rstest]
    fn test_lindera_chinese_tokenizer() {
        let mut tokenizer = LinderaChineseTokenizer::default();
        let tokens = test_helper(
            &mut tokenizer,
            "地址1，包含無效的字元 (包括符號與不標準的asci阿爾發字元",
        );
        // With keep_whitespace=true (backward compatible behavior), whitespace is included as a token
        assert_eq!(tokens.len(), 19);
        {
            let token = &tokens[0];
            assert_eq!(token.text, "地址");
            assert_eq!(token.offset_from, 0);
            assert_eq!(token.offset_to, 6);
            assert_eq!(token.position, 0);
            assert_eq!(token.position_length, 1);
        }
    }

    #[rstest]
    fn test_japanese_tokenizer() {
        let mut tokenizer = LinderaJapaneseTokenizer::default();
        {
            let tokens = test_helper(&mut tokenizer, "すもももももももものうち");
            assert_eq!(tokens.len(), 7);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "すもも");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 9);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
        }
    }

    #[rstest]
    fn test_korean_tokenizer() {
        let mut tokenizer = LinderaKoreanTokenizer::default();
        {
            // With keep_whitespace=true (backward compatible behavior), whitespace is included as tokens
            let tokens = test_helper(&mut tokenizer, "일본입니다. 매우 멋진 단어입니다.");
            assert_eq!(tokens.len(), 11);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "일본");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 6);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
        }
    }

    #[rstest]
    fn test_lindera_chinese_tokenizer_with_empty_string() {
        let mut tokenizer = LinderaChineseTokenizer::default();
        {
            let tokens = test_helper(&mut tokenizer, "");
            assert_eq!(tokens.len(), 0);
        }
        {
            let tokens = test_helper(&mut tokenizer, "    ");
            assert_eq!(tokens.len(), 0);
        }
    }

    #[rstest]
    fn test_japanese_tokenizer_with_empty_string() {
        let mut tokenizer = LinderaJapaneseTokenizer::default();
        {
            let tokens = test_helper(&mut tokenizer, "");
            assert_eq!(tokens.len(), 0);
        }
        {
            let tokens = test_helper(&mut tokenizer, "    ");
            assert_eq!(tokens.len(), 0);
        }
    }

    #[rstest]
    fn test_korean_tokenizer_with_empty_string() {
        let mut tokenizer = LinderaKoreanTokenizer::default();
        {
            let tokens = test_helper(&mut tokenizer, "");
            assert_eq!(tokens.len(), 0);
        }
        {
            let tokens = test_helper(&mut tokenizer, "    ");
            assert_eq!(tokens.len(), 0);
        }
    }

    #[rstest]
    fn test_korean_tokenizer_with_user_dict() {
        use std::io::Write;

        let dict_path = std::env::temp_dir().join("lindera_test_user_dict.csv");
        let mut file = std::fs::File::create(&dict_path).unwrap();
        writeln!(file, "임플란트식립,NNG,임플란트식립").unwrap();
        writeln!(file, "치근단절제술,NNG,치근단절제술").unwrap();
        drop(file);

        let text = "임플란트식립을 진행합니다";

        // Without user dict: "임플란트식립" is split into "임", "플란트", "식", "립"
        {
            let mut tokenizer = LinderaKoreanTokenizer::default();
            let tokens = test_helper(&mut tokenizer, text);
            // "임" "플란트" "식" "립" "을" " " "진행" "합니다"
            assert_eq!(tokens.len(), 8);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "임");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 3);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
            {
                let token = &tokens[1];
                assert_eq!(token.text, "플란트");
                assert_eq!(token.offset_from, 3);
                assert_eq!(token.offset_to, 12);
                assert_eq!(token.position, 1);
                assert_eq!(token.position_length, 1);
            }
        }

        // With user dict: "임플란트식립" stays as one token
        {
            let tok = get_tokenizer_with_user_dict("korean", dict_path.to_str().unwrap());
            let mut tokenizer = LinderaTokenizerWithDict::new(tok);
            let tokens = test_helper(&mut tokenizer, text);
            // "임플란트식립" "을" " " "진행" "합니다"
            assert_eq!(tokens.len(), 5);
            {
                let token = &tokens[0];
                assert_eq!(token.text, "임플란트식립");
                assert_eq!(token.offset_from, 0);
                assert_eq!(token.offset_to, 18);
                assert_eq!(token.position, 0);
                assert_eq!(token.position_length, 1);
            }
            {
                let token = &tokens[3];
                assert_eq!(token.text, "진행");
                assert_eq!(token.offset_from, 22);
                assert_eq!(token.offset_to, 28);
                assert_eq!(token.position, 3);
                assert_eq!(token.position_length, 1);
            }
        }

        let _ = std::fs::remove_file(&dict_path);
    }

    #[rstest]
    fn test_user_dict_tokenizer_with_empty_string() {
        use std::io::Write;

        let dict_path = std::env::temp_dir().join("lindera_test_empty_dict.csv");
        let mut file = std::fs::File::create(&dict_path).unwrap();
        writeln!(file, "테스트,NNG,테스트").unwrap();
        drop(file);

        let tok = get_tokenizer_with_user_dict("korean", dict_path.to_str().unwrap());
        let mut tokenizer = LinderaTokenizerWithDict::new(tok);

        let tokens = test_helper(&mut tokenizer, "");
        assert_eq!(tokens.len(), 0);

        let tokens = test_helper(&mut tokenizer, "    ");
        assert_eq!(tokens.len(), 0);

        let _ = std::fs::remove_file(&dict_path);
    }

    #[rstest]
    fn test_user_dict_tokenizer_caching() {
        use std::io::Write;

        let dict_path = std::env::temp_dir().join("lindera_test_cache_dict.csv");
        let mut file = std::fs::File::create(&dict_path).unwrap();
        writeln!(file, "테스트,NNG,테스트").unwrap();
        drop(file);

        let path_str = dict_path.to_str().unwrap();
        let tok1 = get_tokenizer_with_user_dict("korean", path_str);
        let tok2 = get_tokenizer_with_user_dict("korean", path_str);
        assert!(
            Arc::ptr_eq(&tok1, &tok2),
            "cached tokenizers should be the same Arc instance"
        );

        let _ = std::fs::remove_file(&dict_path);
    }
}
