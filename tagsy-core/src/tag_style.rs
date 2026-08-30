//! A tag's full visual style: the ten peer properties that decide how a tag
//! renders. This is the single source of truth shared by every layer — the
//! wire protocol, the store, the FFI DTO, the CLI, and (mirrored in Dart) the
//! Flutter renderer.
//!
//! Design invariant: **every property is a concrete stored value with a
//! concrete default; nothing is ever derived at render time.** A "derive it
//! from the fill" fallback would be re-implemented slightly differently by each
//! frontend and drift apart, so the stored value is always authoritative and
//! every frontend renders identically from it. See
//! `store::schema::create_tags_v2` for the matching column defaults.

use serde::{Deserialize, Serialize};

/// The outline geometry of a tag's pill. Stored/serialized as its lowercase
/// name (`rounded` / `stadium` / `square` / `cut_corner`) so the wire value is
/// self-describing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TagShape {
    Rounded,
    #[default]
    Stadium,
    Square,
    CutCorner,
}

impl TagShape {
    /// The lowercase wire/SQL name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rounded => "rounded",
            Self::Stadium => "stadium",
            Self::Square => "square",
            Self::CutCorner => "cut_corner",
        }
    }

    /// Parse the lowercase wire/SQL name, falling back to the default for an
    /// unrecognized value (forward-compatible: an older build reading a shape a
    /// newer build wrote renders it as the neutral stadium rather than
    /// failing).
    pub fn from_str_or_default(value: &str) -> Self {
        match value {
            "rounded" => Self::Rounded,
            "stadium" => Self::Stadium,
            "square" => Self::Square,
            "cut_corner" => Self::CutCorner,
            _ => Self::default(),
        }
    }
}

/// How a tag's border is drawn. Stored/serialized as its lowercase name
/// (`none` / `solid` / `dashed`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BorderStyle {
    None,
    #[default]
    Solid,
    Dashed,
}

impl BorderStyle {
    /// The lowercase wire/SQL name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Solid => "solid",
            Self::Dashed => "dashed",
        }
    }

    /// Parse the lowercase wire/SQL name, falling back to the default.
    pub fn from_str_or_default(value: &str) -> Self {
        match value {
            "none" => Self::None,
            "solid" => Self::Solid,
            "dashed" => Self::Dashed,
            _ => Self::default(),
        }
    }
}

pub const DEFAULT_DOT_COLOR: &str = "#000000";
pub const WHITE: &str = "#FFFFFF";
pub const TRANSPARENT: &str = "#00000000";
pub const DEFAULT_FOREGROUND: &str = "#000000";
pub const DEFAULT_SHADOW_COLOR: &str = "#80000000";
pub const DEFAULT_BORDER_WIDTH: f64 = 1.5;

/// A tag's complete visual style. Ten peer properties, none optional, each with
/// a concrete default (see the module docs and [`TagStyle::default`]).
///
/// Colors are hex strings, `#RRGGBB` or `#RRGGBBAA` (the alpha form is used by
/// the ones that default to transparent). The two enums are [`BorderStyle`] and
/// [`TagShape`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TagStyle {
    /// The leading color dot. This is where the old per-tag `color` migrated to
    /// — a tag's identity color — but it is otherwise a peer of the rest.
    pub dot_color: String,
    /// The pill fill. Defaults to transparent (no fill).
    pub background: String,
    /// The color the fill fades *to*, left→right, when it differs from
    /// [`background`](Self::background). Equal to `background` means no visible
    /// gradient (a fade from a color to itself). A gradient is only meaningful
    /// when `background` is not transparent; with a transparent background the
    /// gradient is ignored and the pill renders transparent.
    pub gradient: String,
    /// Text (and icon-less label) color. Defaults to black.
    pub foreground: String,
    /// Border color. Defaults to transparent (no visible border).
    pub border: String,
    /// Border stroke width. Defaults to 1.5.
    pub border_width: f64,
    /// Border stroke style. Defaults to solid.
    pub border_style: BorderStyle,
    /// Pill outline geometry. Defaults to stadium.
    pub shape: TagShape,
    /// Whether to paint a soft drop shadow. Defaults to false.
    pub shadow: bool,
    /// The color of the drop shadow (used when [`shadow`](Self::shadow) is on).
    /// Defaults to a semi-transparent black.
    pub shadow_color: String,
}

impl Default for TagStyle {
    fn default() -> Self {
        Self {
            dot_color: DEFAULT_DOT_COLOR.to_owned(),
            background: WHITE.to_owned(),
            gradient: WHITE.to_owned(),
            foreground: DEFAULT_FOREGROUND.to_owned(),
            border: TRANSPARENT.to_owned(),
            border_width: DEFAULT_BORDER_WIDTH,
            border_style: BorderStyle::Solid,
            shape: TagShape::Stadium,
            shadow: false,
            shadow_color: DEFAULT_SHADOW_COLOR.to_owned(),
        }
    }
}
