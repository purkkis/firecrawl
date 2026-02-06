use lopdf::{Document, Object, ObjectId};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize)]
#[napi(object)]
pub struct PDFMetadata {
  pub num_pages: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[napi(object)]
pub struct PDFAnalysis {
  pub num_pages: i32,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub title: Option<String>,
  pub is_encrypted: bool,
  pub sample_pages: i32,
  pub extracted_char_count: i32,
  pub empty_text_pages: i32,
  pub image_xobject_count: i32,
  pub likely_scanned: bool,
  pub recommended_route: String,
}

impl Default for PDFAnalysis {
  fn default() -> Self {
    PDFAnalysis {
      num_pages: 0,
      title: None,
      is_encrypted: false,
      sample_pages: 0,
      extracted_char_count: 0,
      empty_text_pages: 0,
      image_xobject_count: 0,
      likely_scanned: true,
      recommended_route: "ocr".to_string(),
    }
  }
}

fn get_sample_page_numbers(num_pages: usize) -> Vec<u32> {
  let mut pages: Vec<u32> = Vec::new();
  let mut seen: HashSet<u32> = HashSet::new();

  if num_pages == 0 {
    return pages;
  }

  // First page
  let first = 1u32;
  if seen.insert(first) {
    pages.push(first);
  }

  // Middle page
  if num_pages > 1 {
    let middle = ((num_pages / 2) + 1) as u32;
    if seen.insert(middle) {
      pages.push(middle);
    }
  }

  // Last page
  if num_pages > 2 {
    let last = num_pages as u32;
    if seen.insert(last) {
      pages.push(last);
    }
  }

  pages
}

fn count_image_xobjects_on_page(doc: &Document, page_id: ObjectId) -> i32 {
  let mut count = 0;

  // Get the page dictionary
  let page_dict = match doc.get_dictionary(page_id) {
    Ok(d) => d,
    Err(_) => return 0,
  };

  // Get Resources dictionary
  let resources = match page_dict.get(b"Resources") {
    Ok(Object::Dictionary(d)) => d.clone(),
    Ok(Object::Reference(r)) => match doc.get_dictionary(*r) {
      Ok(d) => d.clone(),
      Err(_) => return 0,
    },
    _ => return 0,
  };

  // Get XObject dictionary from Resources
  let xobjects = match resources.get(b"XObject") {
    Ok(Object::Dictionary(d)) => d.clone(),
    Ok(Object::Reference(r)) => match doc.get_dictionary(*r) {
      Ok(d) => d.clone(),
      Err(_) => return 0,
    },
    _ => return 0,
  };

  // Iterate over XObjects and count images
  for (_name, obj) in xobjects.iter() {
    let obj_id = match obj {
      Object::Reference(r) => *r,
      _ => continue,
    };

    // Try to get the stream
    if let Ok(stream) = doc.get_object(obj_id) {
      if let Object::Stream(s) = stream {
        // Check if it's an Image subtype
        if let Ok(Object::Name(subtype)) = s.dict.get(b"Subtype") {
          if subtype == b"Image" {
            count += 1;
          }
        }
      }
    }
  }

  count
}

fn extract_text_from_page(doc: &Document, page_num: u32) -> Option<String> {
  doc.extract_text(&[page_num]).ok()
}

fn _analyze_pdf(path: &str) -> PDFAnalysis {
  // Load the full document (single load for both metadata and analysis)
  let doc = match Document::load(path) {
    Ok(d) => d,
    Err(_) => {
      // Return safe default for corrupt/unreadable PDF
      return PDFAnalysis::default();
    }
  };

  // Check if encrypted
  let is_encrypted = doc.is_encrypted();
  if is_encrypted {
    // For encrypted PDFs, we can still get basic info but should route to OCR
    let num_pages = doc.get_pages().len() as i32;
    return PDFAnalysis {
      num_pages,
      title: None,
      is_encrypted: true,
      sample_pages: 0,
      extracted_char_count: 0,
      empty_text_pages: 0,
      image_xobject_count: 0,
      likely_scanned: true,
      recommended_route: "ocr".to_string(),
    };
  }

  // Get page count and title
  let pages = doc.get_pages();
  let num_pages = pages.len() as i32;

  // Extract title from document info dictionary
  let title = doc
    .trailer
    .get(b"Info")
    .ok()
    .and_then(|info| match info {
      Object::Reference(r) => doc.get_dictionary(*r).ok(),
      Object::Dictionary(d) => Some(d),
      _ => None,
    })
    .and_then(|info_dict| info_dict.get(b"Title").ok())
    .and_then(|title_obj| match title_obj {
      Object::String(s, _) => String::from_utf8(s.clone()).ok(),
      _ => None,
    })
    .filter(|s| !s.trim().is_empty());

  // Sample up to 3 pages: first, middle, last (dedupe if fewer pages)
  let sample_page_nums = get_sample_page_numbers(num_pages as usize);
  let sample_pages = sample_page_nums.len() as i32;

  // Get page IDs for our sampled pages (pages is BTreeMap<page_num, ObjectId>)
  let page_ids: Vec<ObjectId> = pages.into_values().collect();

  // Extract text and count chars/empty pages
  let mut extracted_char_count = 0i32;
  let mut empty_text_pages = 0i32;

  for &page_num in &sample_page_nums {
    if let Some(text) = extract_text_from_page(&doc, page_num) {
      let char_count = text.chars().filter(|c| !c.is_whitespace()).count() as i32;
      extracted_char_count += char_count;
      if char_count < 10 {
        empty_text_pages += 1;
      }
    } else {
      empty_text_pages += 1;
    }
  }

  // Count image XObjects on sampled pages
  let mut image_xobject_count = 0i32;
  for &page_num in &sample_page_nums {
    let page_idx = (page_num - 1) as usize;
    if page_idx < page_ids.len() {
      image_xobject_count += count_image_xobjects_on_page(&doc, page_ids[page_idx]);
    }
  }

  // Apply heuristics
  let avg_chars = if sample_pages > 0 {
    extracted_char_count / sample_pages
  } else {
    0
  };
  let empty_ratio = if sample_pages > 0 {
    empty_text_pages as f64 / sample_pages as f64
  } else {
    1.0
  };

  let likely_scanned = (avg_chars < 200 && image_xobject_count > 0)
    || (empty_ratio > 0.6 && image_xobject_count > 0);

  // Determine recommended route
  let recommended_route = if likely_scanned {
    "ocr"
  } else if avg_chars >= 200 && image_xobject_count > 0 {
    "layout"
  } else {
    "fast"
  }
  .to_string();

  PDFAnalysis {
    num_pages,
    title,
    is_encrypted,
    sample_pages,
    extracted_char_count,
    empty_text_pages,
    image_xobject_count,
    likely_scanned,
    recommended_route,
  }
}

/// Analyze PDF file for metadata and routing recommendation.
/// Performs both metadata extraction and triage in a single document load.
/// Returns analysis including num_pages, title, and recommended_route ("fast" | "layout" | "ocr").
#[napi]
pub fn analyze_pdf(path: String) -> PDFAnalysis {
  // Catch-all wrapper to ensure we never panic
  std::panic::catch_unwind(|| _analyze_pdf(&path)).unwrap_or_else(|_| PDFAnalysis::default())
}

/// Extract metadata from PDF file.
/// @deprecated Use analyze_pdf instead, which provides both metadata and routing in a single call.
#[napi]
pub fn get_pdf_metadata(path: String) -> Result<PDFMetadata> {
  let analysis = analyze_pdf(path);
  Ok(PDFMetadata {
    num_pages: analysis.num_pages,
    title: analysis.title,
  })
}
