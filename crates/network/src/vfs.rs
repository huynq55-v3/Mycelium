use std::time::{SystemTime, UNIX_EPOCH};
use core_crypto::{decrypt_data, encrypt_data, Identity};
use serde::{Deserialize, Serialize};

/// Thông tin một tệp tin trong cây thư mục ảo (Virtual File System).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileNode {
    pub name: String,
    pub size: u64,
    pub encrypted_cid: String,
    pub encryption_key_hex: String,
    pub k_data_shards: usize,
    pub n_total_shards: usize,
    pub shard_hashes: Vec<String>,
    pub updated_at: u64,
}

/// Một thư mục trong cây thư mục ảo, chứa danh sách đệ quy các entry con.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryNode {
    pub name: String,
    pub entries: Vec<VfsEntry>,
}

impl DirectoryNode {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            entries: Vec::new(),
        }
    }

    pub fn insert_recursive(&mut self, parts: &[&str], file_node: FileNode) -> Result<(), String> {
        if parts.is_empty() {
            return Err("Đường dẫn rỗng".to_string());
        }

        if parts.len() == 1 {
            let file_name = parts[0];
            self.entries.retain(|e| e.name() != file_name);
            let mut final_node = file_node;
            final_node.name = file_name.to_string();
            self.entries.push(VfsEntry::File(final_node));
            return Ok(());
        }

        let dir_name = parts[0];
        let remaining = &parts[1..];

        let idx = match self.entries.iter().position(|e| {
            if let VfsEntry::Dir(d) = e {
                d.name == dir_name
            } else {
                false
            }
        }) {
            Some(i) => i,
            None => {
                let new_dir = DirectoryNode::new(dir_name);
                self.entries.push(VfsEntry::Dir(new_dir));
                self.entries.len() - 1
            }
        };

        if let VfsEntry::Dir(ref mut child_dir) = self.entries[idx] {
            child_dir.insert_recursive(remaining, file_node)
        } else {
            Err(format!("Entry {} không phải là thư mục", dir_name))
        }
    }

    pub fn remove_recursive(&mut self, parts: &[&str]) -> Option<VfsEntry> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            let target_name = parts[0];
            let pos = self.entries.iter().position(|e| e.name() == target_name)?;
            return Some(self.entries.remove(pos));
        }

        let dir_name = parts[0];
        let remaining = &parts[1..];

        let child_dir = self.entries.iter_mut().find_map(|e| {
            if let VfsEntry::Dir(d) = e {
                if d.name == dir_name {
                    Some(d)
                } else {
                    None
                }
            } else {
                None
            }
        })?;

        child_dir.remove_recursive(remaining)
    }

    pub fn find_recursive(&self, parts: &[&str]) -> Option<&FileNode> {
        if parts.is_empty() {
            return None;
        }

        if parts.len() == 1 {
            let target_name = parts[0];
            return self.entries.iter().find_map(|e| {
                if let VfsEntry::File(f) = e {
                    if f.name == target_name {
                        Some(f)
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
        }

        let dir_name = parts[0];
        let remaining = &parts[1..];

        let child_dir = self.entries.iter().find_map(|e| {
            if let VfsEntry::Dir(d) = e {
                if d.name == dir_name {
                    Some(d)
                } else {
                    None
                }
            } else {
                None
            }
        })?;

        child_dir.find_recursive(remaining)
    }
}

/// Mục phần tử trong cây: có thể là Tệp tin (`File`) hoặc Thư mục (`Dir`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum VfsEntry {
    #[serde(rename = "file")]
    File(FileNode),
    #[serde(rename = "dir")]
    Dir(DirectoryNode),
}

impl VfsEntry {
    pub fn name(&self) -> &str {
        match self {
            VfsEntry::File(f) => &f.name,
            VfsEntry::Dir(d) => &d.name,
        }
    }
}

/// Cây thư mục ảo hoàn chỉnh của một người dùng, được mã hóa toàn phần bằng DID Private Key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualTree {
    pub version: u32,
    pub owner_did: String,
    pub updated_at: u64,
    pub root: DirectoryNode,
}

impl VirtualTree {
    /// Khởi tạo cây thư mục ảo rỗng cho một DID.
    pub fn new(owner_did: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            version: 1,
            owner_did: owner_did.to_string(),
            updated_at: now,
            root: DirectoryNode::new("/"),
        }
    }

    /// Thêm hoặc ghi đè một tệp tin vào cây thư mục ảo theo đường dẫn tuyệt đối (ví dụ: `/Documents/report.pdf`).
    pub fn insert_file(&mut self, virtual_path: &str, file_node: FileNode) -> Result<(), String> {
        let parts: Vec<&str> = virtual_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            return Err("Đường dẫn tệp tin không hợp lệ".to_string());
        }

        self.root.insert_recursive(&parts, file_node)?;
        self.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(())
    }

    /// Xóa một tệp tin hoặc thư mục con theo đường dẫn.
    pub fn remove_path(&mut self, virtual_path: &str) -> Result<Option<VfsEntry>, String> {
        let parts: Vec<&str> = virtual_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            return Err("Không thể xóa thư mục gốc /".to_string());
        }

        let res = self.root.remove_recursive(&parts);
        if res.is_some() {
            self.updated_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }
        Ok(res)
    }

    /// Tìm kiếm thông tin `FileNode` theo đường dẫn ảo.
    pub fn find_file(&self, virtual_path: &str) -> Option<&FileNode> {
        let parts: Vec<&str> = virtual_path
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            return None;
        }

        self.root.find_recursive(&parts)
    }

    /// Lấy danh sách toàn bộ các file kèm đường dẫn đầy đủ trong cây.
    pub fn list_all_files(&self) -> Vec<(String, &FileNode)> {
        let mut result = Vec::new();
        Self::collect_files(&self.root, "", &mut result);
        result
    }

    fn collect_files<'a>(dir: &'a DirectoryNode, prefix: &str, out: &mut Vec<(String, &'a FileNode)>) {
        for entry in &dir.entries {
            match entry {
                VfsEntry::File(f) => {
                    let path = if prefix.is_empty() {
                        format!("/{}", f.name)
                    } else {
                        format!("{}/{}", prefix, f.name)
                    };
                    out.push((path, f));
                }
                VfsEntry::Dir(d) => {
                    let path = if prefix.is_empty() {
                        format!("/{}", d.name)
                    } else {
                        format!("{}/{}", prefix, d.name)
                    };
                    Self::collect_files(d, &path, out);
                }
            }
        }
    }

    /// Tính tổng dung lượng file gốc đang lưu trữ trong toàn bộ cây.
    pub fn total_uploaded_bytes(&self) -> u64 {
        self.list_all_files().into_iter().map(|(_, f)| f.size).sum()
    }

    /// Hiển thị cây thư mục phân cấp dạng chuỗi trực quan.
    pub fn render_tree(&self) -> String {
        let mut buffer = String::new();
        buffer.push_str("📁 /\n");
        Self::render_dir(&self.root, "", &mut buffer);
        buffer
    }

    fn render_dir(dir: &DirectoryNode, indent: &str, buffer: &mut String) {
        let count = dir.entries.len();
        for (i, entry) in dir.entries.iter().enumerate() {
            let is_last = i == count - 1;
            let marker = if is_last { "└── " } else { "├── " };
            let child_indent = if is_last { "    " } else { "│   " };

            match entry {
                VfsEntry::File(f) => {
                    let size_kb = f.size as f64 / 1024.0;
                    buffer.push_str(&format!("{}{}{} ({:.1} KB)\n", indent, marker, f.name, size_kb));
                }
                VfsEntry::Dir(d) => {
                    buffer.push_str(&format!("{}{}{}/\n", indent, marker, d.name));
                    let next_indent = format!("{}{}", indent, child_indent);
                    Self::render_dir(d, &next_indent, buffer);
                }
            }
        }
    }

    /// Mã hóa toàn bộ VirtualTree thành byte array bằng SecretKey của Identity.
    pub fn encrypt_tree(&self, identity: &Identity) -> Result<Vec<u8>, String> {
        let json_bytes = serde_json::to_vec(self)
            .map_err(|e| format!("Lỗi serialize VirtualTree: {e}"))?;

        let key = identity.secret_key_bytes();
        encrypt_data(&json_bytes, &key).map_err(|e| format!("Lỗi mã hóa VirtualTree: {e}"))
    }

    /// Giải mã byte array thành VirtualTree bằng SecretKey của Identity.
    pub fn decrypt_tree(encrypted_bytes: &[u8], identity: &Identity) -> Result<Self, String> {
        let key = identity.secret_key_bytes();
        let decrypted_bytes = decrypt_data(encrypted_bytes, &key)
            .map_err(|e| format!("Lỗi giải mã VirtualTree: {e}"))?;

        serde_json::from_slice(&decrypted_bytes)
            .map_err(|e| format!("Lỗi parse VirtualTree sau giải mã: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_virtual_tree_operations_and_encryption() {
        let identity = Identity::generate();
        let mut tree = VirtualTree::new(&identity.to_did());

        let file1 = FileNode {
            name: "notes.txt".to_string(),
            size: 1024,
            encrypted_cid: "cid1".to_string(),
            encryption_key_hex: "key1".to_string(),
            k_data_shards: 10,
            n_total_shards: 40,
            shard_hashes: vec!["h1".to_string(), "h2".to_string()],
            updated_at: 100,
        };

        let file2 = FileNode {
            name: "report.pdf".to_string(),
            size: 2048576,
            encrypted_cid: "cid2".to_string(),
            encryption_key_hex: "key2".to_string(),
            k_data_shards: 10,
            n_total_shards: 40,
            shard_hashes: vec!["h3".to_string(), "h4".to_string()],
            updated_at: 200,
        };

        // Insert /notes.txt
        tree.insert_file("/notes.txt", file1.clone()).unwrap();
        // Insert /Documents/Work/report.pdf (tự tạo /Documents và /Work)
        tree.insert_file("/Documents/Work/report.pdf", file2.clone()).unwrap();

        assert_eq!(tree.list_all_files().len(), 2);
        assert_eq!(tree.total_uploaded_bytes(), 1024 + 2048576);

        // Find file
        let found = tree.find_file("/Documents/Work/report.pdf").unwrap();
        assert_eq!(found.size, 2048576);

        // Render tree
        let rendered = tree.render_tree();
        assert!(rendered.contains("notes.txt"));
        assert!(rendered.contains("Documents/"));
        assert!(rendered.contains("Work/"));
        assert!(rendered.contains("report.pdf"));

        // Encrypt & Decrypt roundtrip
        let encrypted = tree.encrypt_tree(&identity).unwrap();
        let decrypted = VirtualTree::decrypt_tree(&encrypted, &identity).unwrap();
        assert_eq!(tree, decrypted);

        // Remove file
        let removed = tree.remove_path("/Documents/Work/report.pdf").unwrap();
        assert!(removed.is_some());
        assert_eq!(tree.list_all_files().len(), 1);
        assert!(tree.find_file("/Documents/Work/report.pdf").is_none());
    }
}
