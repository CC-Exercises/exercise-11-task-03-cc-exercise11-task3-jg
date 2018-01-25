package de.ustutt.iaas.cc.core;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;

import de.ustutt.iaas.cc.api.Note;
import de.ustutt.iaas.cc.api.NoteWithText;

/**
 * DAO implementation that stores notes as entities in Google Datastore (NoSQL).
 * 
 * @author hauptfn
 *
 */
public class GoogleDatastoreNotebookDAO implements INotebookDAO {
	
    private final static Logger logger = LoggerFactory.getLogger(GoogleDatastoreNotebookDAO.class);
    
    private final static String KIND_NOTE = "Note";
    private final static String NOTE_AUTHOR = "author";
    private final static String NOTE_TEXT = "text";
    
    // Google Cloud Datastore client
    private Datastore datastore;
    private KeyFactory keyFactory;

    public GoogleDatastoreNotebookDAO() {
    	super();
    	// Instantiates a client
    	datastore = DatastoreOptions.getDefaultInstance().getService();
    	keyFactory = datastore.newKeyFactory().setKind(KIND_NOTE);
    }

    @Override
    public Set<Note> getNotes() {
    	Set<Note> result = new HashSet<Note>();
    	
		Query<Entity> query = Query.newEntityQueryBuilder().setKind(KIND_NOTE).build();
		QueryResults<Entity> resultList = datastore.run(query);
		while (resultList.hasNext()) {
			Entity entity = resultList.next();
			Note note = buildNoteFromEntity(entity);
			result.add(note);
		}
    	
		logger.info("Loaded " + result.size() + " notes from google datastore");
		
    	return result;
    }

    @Override
    public NoteWithText getNote(String noteID) {
    	NoteWithText result = null;
    	Key key = keyFactory.newKey(Long.valueOf(noteID));
    	Entity entity = datastore.get(key);
    	result = buildNoteWithTextFromEntity(entity);
    	
    	logger.info("Requested note with id: " + entity.getKey().getId());
    	
    	return result;
    }

	@Override
	public NoteWithText createOrUpdateNote(NoteWithText note) {
		NoteWithText result = null;
		Entity entity = null;
		
		if (note.getId() != null && !note.getId().isEmpty()) {
			Key key = keyFactory.newKey(Long.valueOf(note.getId()));

			entity = Entity.newBuilder(key)
				.set(NOTE_AUTHOR, note.getAuthor())
				.set(NOTE_TEXT, note.getText())
				.build();
			datastore.update(entity);
	        logger.info("Retrieved note from google datastore with id: " + entity.getKey().getId());
			
		} else {
			IncompleteKey key = keyFactory.newKey();
			// Prepares the new entity
			FullEntity<IncompleteKey> newNote = Entity.newBuilder(key)
					.set(NOTE_AUTHOR, note.getAuthor())
					.set(NOTE_TEXT, note.getText())
					.build();
			entity = datastore.add(newNote);
			
			logger.info("Created new note on google datastore with id: " + entity.getKey().getId());
		}

		result = buildNoteWithTextFromEntity(entity);
		return result;
	}

    @Override
    public void deleteNote(String noteID) {
    	Key key = keyFactory.newKey(Long.valueOf(noteID));
    	datastore.delete(key); 
    	logger.info("Delete note with id: " + noteID);
    }

    
    /**
     * Build Note object from Entity.
     * @param entity
     * @return
     */
    private Note buildNoteFromEntity(Entity entity) {
    	String author = entity.getString(NOTE_AUTHOR);
		String id = String.valueOf(entity.getKey().getId());
		return new Note(id, author);
    }
    
    /**
     * Build NoteWithText object from Entity.
     * @param entity
     * @return
     */
    private NoteWithText buildNoteWithTextFromEntity(Entity entity) {
    	String author = entity.getString(NOTE_AUTHOR);
		String text = entity.getString(NOTE_TEXT);
		String id = String.valueOf(entity.getKey().getId());
		return new NoteWithText(id, author, text);
    }
    
}
