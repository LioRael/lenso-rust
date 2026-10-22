use crate::{Error, Limits, Preset, Result};
use image::{GenericImageView, ImageDecoder, ImageFormat, ImageReader};
use std::io::{Cursor, Seek, SeekFrom, Write};

pub(crate) fn process(
    bytes: Vec<u8>,
    preset: Option<Preset>,
    limits: Limits,
) -> Result<(u32, u32, Vec<u8>)> {
    let mut reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| Error::Image)?;
    if !matches!(reader.format(), Some(ImageFormat::Png | ImageFormat::Jpeg)) {
        return Err(Error::Image);
    }
    let mut decoder_limits = image::Limits::default();
    decoder_limits.max_image_width = Some(limits.max_dimension);
    decoder_limits.max_image_height = Some(limits.max_dimension);
    decoder_limits.max_alloc = Some(limits.memory_bytes);
    reader.limits(decoder_limits);
    let decoder = reader.into_decoder().map_err(|_| Error::Image)?;
    let (w, h) = decoder.dimensions();
    limits.check_dimensions(w, h)?;
    let Some(preset) = preset else {
        return Ok((w, h, Vec::new()));
    };
    let mut img = image::DynamicImage::from_decoder(decoder).map_err(|_| Error::Image)?;
    if let Some([x, y, cw, ch]) = preset.crop {
        if cw == 0
            || ch == 0
            || x.checked_add(cw).is_none_or(|v| v > w)
            || y.checked_add(ch).is_none_or(|v| v > h)
        {
            return Err(Error::Invalid);
        }
        img = img.crop_imm(x, y, cw, ch);
    }
    let img = img.resize(
        preset.width,
        preset.height,
        image::imageops::FilterType::Triangle,
    );
    let dimensions = img.dimensions();
    let img = if matches!(preset.format, crate::Format::Jpeg) {
        image::DynamicImage::ImageRgb8(img.to_rgb8())
    } else {
        img
    };
    let mut out = Bounded {
        data: Cursor::new(Vec::new()),
        max: limits.output_bytes,
    };
    img.write_to(&mut out, preset.format.image())
        .map_err(|_| Error::Image)?;
    let result = out.data.into_inner();
    Ok((dimensions.0, dimensions.1, result))
}
struct Bounded {
    data: Cursor<Vec<u8>>,
    max: u64,
}
impl Write for Bounded {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.data.position().saturating_add(buf.len() as u64) > self.max {
            return Err(std::io::Error::other("output limit"));
        }
        self.data.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Seek for Bounded {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.data.seek(pos)
    }
}
