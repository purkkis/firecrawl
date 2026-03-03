use napi::bindgen_prelude::*;
use napi_derive::napi;
use pdf_inspector::{
  MarkdownOptions, PdfOptions, PdfType,
  process_pdf_with_options as rust_process_pdf,
};

#[napi(object)]
pub struct PdfProcessResult {
  pub pdf_type: String,
  pub markdown: Option<String>,
  pub page_count: i32,
  pub processing_time_ms: f64,
  pub pages_needing_ocr: Vec<i32>,
  pub title: Option<String>,
  pub confidence: f64,
  pub is_complex: bool,
}

fn pdf_type_str(t: PdfType) -> &'static str {
  match t {
    PdfType::TextBased => "TextBased",
    PdfType::Scanned => "Scanned",
    PdfType::ImageBased => "ImageBased",
    PdfType::Mixed => "Mixed",
  }
}

fn to_napi_result(result: pdf_inspector::PdfProcessResult) -> PdfProcessResult {
  PdfProcessResult {
    pdf_type: pdf_type_str(result.pdf_type).to_string(),
    markdown: result.markdown,
    page_count: result.page_count as i32,
    processing_time_ms: result.processing_time_ms as f64,
    pages_needing_ocr: result.pages_needing_ocr.iter().map(|&p| p as i32).collect(),
    title: result.title,
    confidence: result.confidence as f64,
    is_complex: result.layout.is_complex,
  }
}

/// Process a PDF file: detect type, extract text + markdown if text-based.
#[napi]
pub fn process_pdf(path: String) -> Result<PdfProcessResult> {
  let result = rust_process_pdf(&path, PdfOptions::new()).map_err(|e| {
    Error::new(
      Status::GenericFailure,
      format!("Failed to process PDF: {e}"),
    )
  })?;

  Ok(to_napi_result(result))
}

/// Process a PDF with page markers (<!-- Page N -->) inserted between pages.
/// Optionally filters to only the specified 1-indexed text pages.
#[napi]
pub fn process_pdf_with_page_markers(
  path: String,
  text_pages: Option<Vec<i32>>,
) -> Result<PdfProcessResult> {
  let mut options = PdfOptions::new().markdown(MarkdownOptions {
    include_page_numbers: true,
    ..Default::default()
  });

  if let Some(pages) = text_pages {
    options = options.pages(pages.into_iter().map(|p| p as u32));
  }

  let result = rust_process_pdf(&path, options).map_err(|e| {
    Error::new(
      Status::GenericFailure,
      format!("Failed to process PDF with page markers: {e}"),
    )
  })?;

  Ok(to_napi_result(result))
}

/// Fast metadata-only detection: page count, title, type, confidence.
/// Skips text extraction, markdown generation, and layout analysis.
#[napi]
pub fn detect_pdf(path: String) -> Result<PdfProcessResult> {
  let result = rust_process_pdf(&path, PdfOptions::detect_only()).map_err(|e| {
    Error::new(
      Status::GenericFailure,
      format!("Failed to detect PDF: {e}"),
    )
  })?;

  Ok(to_napi_result(result))
}
