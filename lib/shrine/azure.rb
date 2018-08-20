require "shrine/azure/version"
require "azure/storage"

class Shrine
  module Storage
    class Azure
      attr_reader :client, :blobs, :container, :signer


      def initialize(storage_account_name, storage_access_key, container)
        @client = ::Azure::Storage::Client.create(storage_account_name: storage_account_name, storage_access_key: storage_access_key)
        @signer = ::Azure::Storage::Core::Auth::SharedAccessSignature.new(storage_account_name, storage_access_key)
        @blobs = @client.blob_client
        @container = container
      end

      def upload(io, id, shrine_metadata: {}, **upload_options)
        # uploads `io` to the location `id`, can accept upload options
        begin
          blobs.create_block_blob(container, id, io.to_io)
        rescue Azure::Core::Http::HTTPError
          raise Shrine::Error
        end
      end

      def open(id, expires_in = nil, **options)
        # returns the remote file as an IO-like object
      end

      def url(id, expires_in = nil, **options)
        # returns URL to the remote file, can accept URL options
        generated_url = signer.signed_uri(
            uri_for(id), false,
            service: "b",
            permissions: "rw",
            expiry: format_expiry(expires_in)
        ).to_s

        generated_url

      end

      def download(id, &block)
        binding.pry
        if block_given?
          stream(id, &block)
        else
          _, io = blobs.get_blob(container, id)
          io.force_encoding(Encoding::BINARY)
        end
      end

      def exists?(id)
        # returns whether the file exists on storage
        blob_for(key).present?
      end

      def delete(id)
        # deletes the file from the storage
        begin
          blobs.delete_blob(container, id)
        rescue Azure::Core::Http::HTTPError
          # Ignore files already deleted
        end
      end

      private
      def format_expiry(expires_in)
        expires_in ? Time.now.utc.advance(seconds: expires_in).iso8601 : nil
      end

      def uri_for(key)
        blobs.generate_uri("#{container}/#{key}")
      end

      # Reads the object for the given key in chunks, yielding each to the block.
      def stream(key)
        binding.pry
        blob = blob_for(key)

        chunk_size = 5.megabytes
        offset = 0

        while offset < blob.properties[:content_length]
          _, chunk = blobs.get_blob(container, key, start_range: offset, end_range: offset + chunk_size - 1)
          yield chunk.force_encoding(Encoding::BINARY)
          offset += chunk_size
        end
      end
    end
  end
end
